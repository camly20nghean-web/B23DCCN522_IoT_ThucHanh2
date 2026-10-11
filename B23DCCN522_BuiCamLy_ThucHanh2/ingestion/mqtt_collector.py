"""
==============================================================================
BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
Học viện Công nghệ Bưu chính Viễn thông (PTIT)
Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
Module: Ingestion Client & Data Validator (Bộ thu thập và kiểm định dữ liệu)
Chức năng:
  1. Subscribe MQTT Topic từ Broker (Wokwi / Real Board / Virtual Sensor).
  2. Validate cấu trúc JSON, kiểu dữ liệu, ngưỡng vật lý sơ cấp.
  3. Lọc bỏ gói tin trùng lặp (De-duplication) & ghi nhận tỉ lệ rớt gói (Loss tracking).
  4. Tính toán độ trễ Ingestion: t_ingest - t_sent.
  5. Ghi real-time vào InfluxDB 2.x (Measurement: sensor_raw).
==============================================================================
"""

import time
import json
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
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# 1. Đọc cấu hình hệ thống
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_TOPIC = config["mqtt"]["topic_raw"]
MQTT_QOS = config["mqtt"]["qos"]

INFLUX_URL = config["influxdb"]["url"]
INFLUX_TOKEN = config["influxdb"]["token"]
INFLUX_ORG = config["influxdb"]["org"]
INFLUX_BUCKET = config["influxdb"]["bucket_raw"]
MEASUREMENT_RAW = config["influxdb"]["measurement_raw"]

VAL_CFG = config["validation"]

# 2. Khởi tạo InfluxDB Client
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# 3. Biến trạng thái để kiểm soát trùng lặp và tính độ trễ
recent_seq_cache = set()
MAX_CACHE_SIZE = 1000
last_expected_seq = {}
stats = {
    "total_received": 0,
    "valid_points": 0,
    "invalid_points": 0,
    "duplicates_dropped": 0,
    "packets_lost": 0,
    "total_latency_ms": 0.0
}

def validate_payload(data: dict):
    """
    Kiểm tra tính hợp lệ của gói tin:
    - Bắt buộc phải có các trường cốt lõi
    - Kiểm tra giới hạn vật lý logic (ngưỡng hợp lý)
    """
    reasons = []

    # Kiểm tra trường bắt buộc
    required_fields = ["device_id", "timestamp", "message_seq"]
    for field in required_fields:
        if field not in data or data[field] is None:
            reasons.append(f"Thiếu trường '{field}'")
            return False, reasons

    # Kiểm tra kiểu dữ liệu sequence và timestamp
    if not isinstance(data.get("message_seq"), (int, float)):
        reasons.append("message_seq không đúng định dạng số")
    if not isinstance(data.get("timestamp"), (int, float)):
        reasons.append("timestamp không đúng định dạng số")

    # Kiểm tra giá trị cảm biến nếu có
    temp = data.get("temperature")
    if temp is not None:
        if not isinstance(temp, (int, float)) or not (VAL_CFG["temp_min"] <= temp <= VAL_CFG["temp_max"]):
            reasons.append(f"Nhiệt độ ngoài ngưỡng ({temp} °C)")

    hum = data.get("humidity")
    if hum is not None:
        if not isinstance(hum, (int, float)) or not (VAL_CFG["humidity_min"] <= hum <= VAL_CFG["humidity_max"]):
            reasons.append(f"Độ ẩm ngoài ngưỡng ({hum} %)")

    light = data.get("light")
    if light is not None:
        if not isinstance(light, (int, float)) or not (VAL_CFG["light_min"] <= light <= VAL_CFG["light_max"]):
            reasons.append(f"Ánh sáng ngoài ngưỡng ({light} Lux)")

    is_valid = len(reasons) == 0
    return is_valid, reasons

def save_to_influxdb(data: dict, is_valid: bool, latency_ms: float):
    """
    Tạo InfluxDB Point với Schema chuẩn time-series:
    - Measurement: sensor_raw
    - Tags: device_id, location, status
    - Fields: temperature, humidity, light, message_seq, latency_ingest_ms
    - Timestamp: Thời gian gửi từ thiết bị
    """
    device_id = str(data.get("device_id", "UNKNOWN"))
    location = str(data.get("location", "UNKNOWN"))
    ts_sec = float(data.get("timestamp", time.time()))

    point = Point(MEASUREMENT_RAW) \
        .tag("device_id", device_id) \
        .tag("location", location) \
        .tag("status", "VALID" if is_valid else "INVALID")

    # Ghi fields nếu có dữ liệu hợp lệ
    for field_name in ["temperature", "humidity", "light"]:
        val = data.get(field_name)
        if val is not None and isinstance(val, (int, float)):
            point.field(field_name, float(val))

    seq = data.get("message_seq")
    if seq is not None:
        point.field("message_seq", int(seq))

    point.field("latency_ingest_ms", float(latency_ms))

    # Gán timestamp nano giây
    point.time(int(ts_sec * 1e9), WritePrecision.NS)

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"✅ Đã kết nối MQTT Broker thành công: {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC, qos=MQTT_QOS)
        print(f"📥 Đang lắng nghe trên topic: {MQTT_TOPIC}\n")
    else:
        print(f"❌ Kết nối Broker thất bại, mã lỗi: {rc}")

def on_message(client, userdata, msg):
    recv_time = time.time()
    stats["total_received"] += 1

    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
    except Exception as e:
        stats["invalid_points"] += 1
        print(f"⚠️ [LỖI PARSE JSON] Gói tin không đúng định dạng: {e}")
        return

    device_id = data.get("device_id", "UNKNOWN")
    seq = data.get("message_seq")
    sent_ts = data.get("timestamp", recv_time)

    # 1. Tính toán độ trễ Ingestion (t_ingest - t_sent)
    latency_ms = max(0.0, (recv_time - sent_ts) * 1000.0)
    stats["total_latency_ms"] += latency_ms

    # 2. Xử lý khử trùng lặp (De-duplication)
    dup_key = f"{device_id}_{seq}_{sent_ts}"
    if dup_key in recent_seq_cache:
        stats["duplicates_dropped"] += 1
        print(f"🔄 [LOẠI TRÙNG LẶP] Bỏ qua gói lặp Seq #{seq} từ {device_id}")
        return

    recent_seq_cache.add(dup_key)
    if len(recent_seq_cache) > MAX_CACHE_SIZE:
        recent_seq_cache.pop()

    # 3. Theo dõi mất mát gói tin (Packet Loss)
    if seq is not None and isinstance(seq, (int, float)):
        if device_id in last_expected_seq:
            expected = last_expected_seq[device_id] + 1
            if seq > expected:
                lost_count = int(seq - expected)
                stats["packets_lost"] += lost_count
                print(f"📉 [PHÁT HIỆN MẤT GÓI] Thiết bị {device_id} mất {lost_count} gói (Từ #{expected} đến #{seq-1})")
        last_expected_seq[device_id] = seq

    # 4. Kiểm tra hợp lệ (Data Validation)
    is_valid, reasons = validate_payload(data)

    if is_valid:
        stats["valid_points"] += 1
        status_tag = "✅ VALID"
    else:
        stats["invalid_points"] += 1
        status_tag = f"❌ INVALID ({', '.join(reasons)})"

    # 5. Lưu vào Database Time-Series
    try:
        save_to_influxdb(data, is_valid, latency_ms)
        print(f"[{time.strftime('%H:%M:%S')}] {status_tag:32s} | "
              f"Dev: {device_id} | Seq: #{seq} | "
              f"T: {data.get('temperature')}°C | H: {data.get('humidity')}% | L: {data.get('light')} Lux | "
              f"Latency: {latency_ms:.1f}ms")
    except Exception as e:
        print(f"❌ [LỖI GHI INFLUXDB]: {e}")

def main():
    print("=" * 70)
    print("🚀 BẮT ĐẦU DỊCH VỤ THU THẬP DỮ LIỆU IoT (INGESTION COLLECTOR)")
    print(f"📌 InfluxDB: {INFLUX_URL} | Bucket: {INFLUX_BUCKET} | Org: {INFLUX_ORG}")
    print(f"📡 MQTT Broker: {MQTT_BROKER}:{MQTT_PORT} | Topic: {MQTT_TOPIC}")
    print("=" * 70)

    client_id = f"{config['mqtt']['client_id_prefix']}_{int(time.time())}"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print("📊 TỔNG KẾT PHIÊN THU THẬP DỮ LIỆU:")
        print(f" - Tổng số gói nhận được : {stats['total_received']}")
        print(f" - Số gói hợp lệ         : {stats['valid_points']}")
        print(f" - Số gói không hợp lệ   : {stats['invalid_points']}")
        print(f" - Số gói trùng lặp loại : {stats['duplicates_dropped']}")
        print(f" - Số gói phát hiện mất  : {stats['packets_lost']}")
        avg_lat = (stats['total_latency_ms'] / stats['total_received']) if stats['total_received'] > 0 else 0
        print(f" - Độ trễ trung bình     : {avg_lat:.2f} ms")
        print("=" * 70)
    finally:
        client.disconnect()
        influx_client.close()
        print("🔌 Đã đóng kết nối Collector và InfluxDB.")

if __name__ == "__main__":
    main()
