"""
==============================================================================
BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
Học viện Công nghệ Bưu chính Viễn thông (PTIT)
Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
Module: Virtual Sensor Simulator (Bộ giả lập cảm biến thông minh)
Chức năng: Phát dữ liệu thời gian thực lên MQTT Broker, chủ động mô phỏng
           dữ liệu bình thường + các trường hợp nhiễu (outlier), mất mát (missing)
           và trùng lặp (duplicate) để kiểm chứng pipeline tiền xử lý.
==============================================================================
"""

import time
import json
import random
import math
import os
import sys
import yaml

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
import paho.mqtt.client as mqtt

# Đọc cấu hình từ file config.yaml
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

BROKER = config["mqtt"]["broker"]
PORT = config["mqtt"]["port"]
TOPIC = config["mqtt"]["topic_raw"]
QOS = config["mqtt"]["qos"]

DEVICE_ID = config["simulation"]["device_id"]
LOCATION = config["simulation"]["location"]
INTERVAL = config["simulation"]["interval_seconds"]
OUTLIER_PROB = config["simulation"]["outlier_prob"]
MISSING_PROB = config["simulation"]["missing_prob"]

def create_mqtt_client():
    client_id = f"virtual_sensor_{random.randint(1000, 9999)}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    return client

def generate_sensor_reading(seq: int):
    """
    Sinh dữ liệu mô phỏng theo hàm sin ngày/đêm kèm nhiễu trắng Gaussian.
    Chủ động bơm Outlier và Missing Value theo xác suất cấu hình.
    """
    now = time.time()
    # Dao động nhiệt độ 25°C - 32°C theo thời gian
    base_temp = 28.0 + 3.0 * math.sin(now / 60.0) + random.gauss(0, 0.4)
    # Độ ẩm tỉ lệ nghịch với nhiệt độ (60% - 75%)
    base_hum = 68.0 - 5.0 * math.sin(now / 60.0) + random.gauss(0, 0.8)
    # Ánh sáng dao động 300 - 800 Lux
    base_light = 500.0 + 200.0 * math.sin(now / 40.0) + random.gauss(0, 20)

    is_outlier = False
    is_missing = False

    # 1. Kịch bản Outlier (Dị thường đột biến)
    if random.random() < OUTLIER_PROB:
        is_outlier = True
        choice = random.choice(["temp_high", "temp_low", "light_spike", "hum_invalid"])
        if choice == "temp_high":
            base_temp += random.uniform(35.0, 60.0)  # > 65°C bất thường
        elif choice == "temp_low":
            base_temp -= random.uniform(35.0, 50.0)  # < 0°C bất thường
        elif choice == "light_spike":
            base_light = random.uniform(4000.0, 8000.0)
        elif choice == "hum_invalid":
            base_hum = random.choice([-15.0, 140.0]) # Vượt ngưỡng vật lý 0-100%

    # 2. Kịch bản Missing Value (Khuyết thiếu dữ liệu)
    temp_val = round(base_temp, 2)
    hum_val = round(base_hum, 2)
    light_val = round(base_light, 1)

    if random.random() < MISSING_PROB:
        is_missing = True
        target = random.choice(["temperature", "humidity", "light"])
        if target == "temperature":
            temp_val = None
        elif target == "humidity":
            hum_val = None
        else:
            light_val = None

    payload = {
        "device_id": DEVICE_ID,
        "location": LOCATION,
        "message_seq": seq,
        "timestamp": round(now, 3),
        "temperature": temp_val,
        "humidity": hum_val,
        "light": light_val,
        "_sim_tag": "OUTLIER" if is_outlier else ("MISSING" if is_missing else "NORMAL")
    }
    return payload, is_outlier, is_missing

def main():
    print("=" * 65)
    print("🚀 KHỞI ĐỘNG VIRTUAL SENSOR SIMULATOR (B23DCCN522)")
    print(f"📡 Broker: {BROKER}:{PORT}")
    print(f"📤 Topic : {TOPIC}")
    print(f"⏱  Chu kỳ: {INTERVAL}s/lần | Outlier rate: {OUTLIER_PROB*100}% | Missing rate: {MISSING_PROB*100}%")
    print("=" * 65)

    client = create_mqtt_client()
    try:
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()
        print("✅ Đã kết nối MQTT Broker thành công! Bắt đầu phát dữ liệu...\n")

        seq = 1
        while True:
            payload, is_outlier, is_missing = generate_sensor_reading(seq)
            msg_str = json.dumps(payload)

            client.publish(TOPIC, msg_str, qos=QOS)

            tag = "⚪ NORMAL"
            if is_outlier:
                tag = "🔴 [BƠM OUTLIER]"
            elif is_missing:
                tag = "🟡 [BƠM MISSING]"

            print(f"[{time.strftime('%H:%M:%S')}] Seq #{seq:04d} | {tag:18s} | "
                  f"Temp: {str(payload['temperature']):>6} °C | "
                  f"Hum: {str(payload['humidity']):>6} % | "
                  f"Light: {str(payload['light']):>6} Lux")

            # Mô phỏng gói trùng lặp (Duplicate) thỉnh thoảng xuất hiện (1%)
            if random.random() < 0.02:
                time.sleep(0.1)
                client.publish(TOPIC, msg_str, qos=QOS)
                print(f"   ↳ 🔁 [MÔ PHỎNG DUPLICATE GÓI #{seq}] Đã gửi lại cùng sequence!")

            seq += 1
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("\n🛑 Người dùng dừng bộ giả lập.")
    finally:
        client.loop_stop()
        client.disconnect()
        print("🔌 Đã ngắt kết nối MQTT Broker.")

if __name__ == "__main__":
    main()
