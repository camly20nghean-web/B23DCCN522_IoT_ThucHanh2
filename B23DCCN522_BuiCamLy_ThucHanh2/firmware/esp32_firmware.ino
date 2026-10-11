/*
 * ==============================================================================
 * BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT (INT14149)
 * Học viện Công nghệ Bưu chính Viễn thông (PTIT)
 * Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
 * Firmware: ESP32 + DHT22 + Quang trở (LDR) -> MQTT Broker
 * Hỗ trợ nạp board thực tế và mô phỏng trực tiếp trên Wokwi (Wokwi-GUEST)
 * ==============================================================================
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <DHT.h>
#include <time.h>

// --- CẤU HÌNH WIFI ---
// Trên Wokwi: SSID là "Wokwi-GUEST", mật khẩu để trống ""
// Trên mạch thật: Thay bằng tên Wi-Fi và mật khẩu thực tế của bạn
const char* WIFI_SSID = "Wokwi-GUEST";
const char* WIFI_PASS = "";

// --- CẤU HÌNH MQTT BROKER ---
const char* MQTT_BROKER = "broker.emqx.io";
const int   MQTT_PORT   = 1883;
const char* MQTT_TOPIC  = "iot/b23dccn522/sensor/raw";
const char* DEVICE_ID   = "ESP32_WOKWI_01";
const char* LOCATION    = "PTIT_LAB_IOT";

// --- CẤU HÌNH CẢM BIẾN ---
#define DHTPIN 15          // Chân Data của DHT22 nối GPIO 15
#define DHTTYPE DHT22      // Loại cảm biến DHT22 (hoặc DHT11)
#define LDRPIN 34          // Chân quang trở analog nối GPIO 34

DHT dht(DHTPIN, DHTTYPE);
WiFiClient espClient;
PubSubClient mqttClient(espClient);

unsigned long lastMsgTime = 0;
const unsigned long MSG_INTERVAL = 3000; // 3 giây gửi dữ liệu 1 lần
unsigned long messageSeq = 0;

// Hàm lấy thời gian Unix timestamp (giây) qua NTP
unsigned long getEpochTime() {
  time_t now;
  struct tm timeinfo;
  if (!getLocalTime(&timeinfo)) {
    // Nếu chưa sync NTP, dùng millis() bù đắp
    return 1728643200 + (millis() / 1000);
  }
  time(&now);
  return (unsigned long)now;
}

void setupWifi() {
  delay(10);
  Serial.println();
  Serial.print("[WiFi] Đang kết nối tới: ");
  Serial.println(WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  int retries = 0;
  while (WiFi.status() != WL_CONNECTED && retries < 20) {
    delay(500);
    Serial.print(".");
    retries++;
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("\n[WiFi] Kết nối thành công!");
    Serial.print("[WiFi] Địa chỉ IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("\n[WiFi] Không thể kết nối. Sẽ thử lại sau.");
  }

  // Cấu hình đồng bộ giờ NTP
  configTime(7 * 3600, 0, "pool.ntp.org", "time.nist.gov");
}

void reconnectMqtt() {
  while (!mqttClient.connected()) {
    Serial.print("[MQTT] Đang kết nối tới Broker: ");
    Serial.print(MQTT_BROKER);
    Serial.print("...");

    String clientId = "ESP32_B23DCCN522_";
    clientId += String(random(0xffff), HEX);

    if (mqttClient.connect(clientId.c_str())) {
      Serial.println(" Thành công!");
      Serial.print("[MQTT] Sẵn sàng phát dữ liệu lên topic: ");
      Serial.println(MQTT_TOPIC);
    } else {
      Serial.print(" Thất bại, rc=");
      Serial.print(mqttClient.state());
      Serial.println(". Thử lại sau 3 giây...");
      delay(3000);
    }
  }
}

void setup() {
  Serial.begin(115200);
  Serial.println("\n=============================================");
  Serial.println("  ESP32 SENSOR NODE - B23DCCN522 (PTIT)      ");
  Serial.println("=============================================");

  dht.begin();
  pinMode(LDRPIN, INPUT);

  setupWifi();
  mqttClient.setServer(MQTT_BROKER, MQTT_PORT);
}

void loop() {
  if (WiFi.status() != WL_CONNECTED) {
    setupWifi();
  }

  if (!mqttClient.connected()) {
    reconnectMqtt();
  }
  mqttClient.loop();

  unsigned long now = millis();
  if (now - lastMsgTime > MSG_INTERVAL) {
    lastMsgTime = now;
    messageSeq++;

    // 1. Đọc dữ liệu cảm biến
    float temperature = dht.readTemperature();
    float humidity = dht.readHumidity();
    int rawLdr = analogRead(LDRPIN);
    // Chuẩn hóa quang trở LDR (0 - 4095) thành Lux xấp xỉ (0 - 1000 lux)
    float light = map(rawLdr, 0, 4095, 1000, 0);

    // Xử lý nếu cảm biến đọc lỗi
    if (isnan(temperature) || isnan(humidity)) {
      Serial.println("[CẢNH BÁO] Không đọc được dữ liệu từ DHT22!");
      temperature = 26.5; // Giá trị fallback an toàn
      humidity = 60.0;
    }

    // Lấy timestamp có phần thập phân mili-giây để tính chính xác độ trễ
    unsigned long epochSec = getEpochTime();
    unsigned long epochMs = millis() % 1000;
    char timestampStr[32];
    snprintf(timestampStr, sizeof(timestampStr), "%lu.%03lu", epochSec, epochMs);

    // 2. Đóng gói dữ liệu định dạng JSON chuẩn
    char jsonPayload[256];
    snprintf(jsonPayload, sizeof(jsonPayload),
      "{\"device_id\":\"%s\",\"location\":\"%s\",\"message_seq\":%lu,\"timestamp\":%s,\"temperature\":%.2f,\"humidity\":%.2f,\"light\":%.1f}",
      DEVICE_ID, LOCATION, messageSeq, timestampStr, temperature, humidity, light
    );

    // 3. Publish lên MQTT Broker
    boolean success = mqttClient.publish(MQTT_TOPIC, jsonPayload);
    if (success) {
      Serial.print("[PUBLISH #");
      Serial.print(messageSeq);
      Serial.print("] Payload: ");
      Serial.println(jsonPayload);
    } else {
      Serial.println("[LỖI] Publish MQTT thất bại!");
    }
  }
}
