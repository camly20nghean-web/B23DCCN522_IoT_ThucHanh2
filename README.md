# BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
**Học phần:** IoT và Ứng dụng (INT14149) - Học viện Công nghệ Bưu chính Viễn thông (PTIT)  
**Sinh viên thực hiện:** Bùi Cẩm Ly  
**Mã sinh viên:** `B23DCCN522`  
**Nhóm:** 04

---

## 📖 1. Giới thiệu tổng quan
Dự án triển khai một hệ thống xử lý dữ liệu IoT hoàn chỉnh theo tiêu chuẩn công nghiệp (End-to-End IoT Data Pipeline):
1. **Thu thập dữ liệu thời gian thực (Sensing & Ingestion):** Board ESP32 (chạy code C++ trên mô phỏng Wokwi) và module Virtual Sensor phát dữ liệu cảm biến (Nhiệt độ, Độ ẩm, Ánh sáng) đóng gói JSON chuẩn lên MQTT Broker (`broker.emqx.io`).
2. **Kiểm tra tính hợp lệ & Lưu trữ Time-Series:** Client Python subscribe dữ liệu, lọc bỏ gói tin lỗi/ngoài ngưỡng vật lý, chống trùng lặp gói tin (De-duplication), đo độ trễ Ingestion và ghi vào **InfluxDB 2.7**.
3. **Pipeline Tiền xử lý dữ liệu chuyên sâu (Data Preprocessing):**
   - Xử lý giá trị thiếu (Missing values) bằng phương pháp nội suy tuyến tính (Linear Interpolation).
   - Phát hiện và xử lý dị thường bằng cả **IQR (Interquartile Range)** và **Z-Score ($|Z| > 3.0$)**, làm mịn bằng Rolling Median.
   - Tái lấy mẫu (Resampling) theo cửa sổ thời gian 1 phút / 5 phút.
   - Trích xuất đặc trưng (Feature Engineering): `rolling_mean`, `rolling_std`, `delta`.
   - Chuẩn hóa dữ liệu về thang $[0, 1]$ bằng **Min-Max Normalization**.
   - Ghi dữ liệu sạch vào measurement mới: `sensor_processed`.
4. **Giám sát thời gian thực (Interactive Dashboard):** Xây dựng trên **Streamlit**, hiển thị đồ thị Real-time, đối chiếu Dữ liệu thô vs Dữ liệu đã làm sạch, cảnh báo Outlier, phân tích phân bố độ trễ End-to-End.
5. **Báo cáo chuẩn học phần:** File Word 4–6 trang được sinh tự động kèm biểu đồ số liệu thực nghiệm.

---

## 📁 2. Cấu trúc thư mục dự án

```text
B23DCCN522_Bùi Cẩm Ly_Thực hành 2/
├── bin/
│   └── influxd.exe               # File thực thi InfluxDB 2.7 (Standalone cho Windows)
├── config/
│   └── config.yaml               # File cấu hình tập trung (Broker, InfluxDB, Validation, Preprocessing)
├── firmware/
│   ├── esp32_firmware.ino        # Mã nguồn C++ cho ESP32 nạp board thật hoặc Wokwi
│   └── diagram.json              # Sơ đồ mạch điện tử mô phỏng Wokwi
├── simulator/
│   └── virtual_sensor.py         # Bộ giả lập cảm biến phát MQTT (bơm nhiễu, outlier, missing để test)
├── ingestion/
│   └── mqtt_collector.py         # Client subscribe MQTT, validate dữ liệu, ghi vào InfluxDB
├── preprocessing/
│   └── data_preprocessor.py      # Pipeline tiền xử lý dữ liệu (Làm sạch, IQR, Z-score, Resample)
├── dashboard/
│   └── app.py                    # Giao diện Dashboard Real-time bằng Streamlit
├── analysis/
│   └── latency_evaluator.py      # Script đo độ trễ End-to-End và xuất ảnh biểu đồ phân tích
├── report/
│   ├── Bao_cao_Thuc_hanh_so_2_B23DCCN522.docx  # Báo cáo Word hoàn chỉnh 4-6 trang
│   └── images/                   # Thư mục lưu các biểu đồ thực nghiệm
├── start_influxdb.bat            # Phím tắt 1-click khởi động InfluxDB Database
├── requirements.txt              # Danh sách thư viện Python cần thiết
                  
```

---

## 🚀 3. Hướng dẫn chạy hệ thống từ A - Z

### Bước 1: Khởi động Time-Series Database (InfluxDB 2.7)
Nhấp đúp chuột vào file:
```bash
start_influxdb.bat
```
*(Hoặc chạy lệnh trong terminal: `.\bin\influxd.exe`)*  
Database InfluxDB sẽ khởi động tại địa chỉ: `http://localhost:8086`.  
- **Tài khoản đăng nhập Web UI:** `admin`  
- **Mật khẩu:** `AdminPassword123!`  
- **Bucket:** `iot_telemetry` | **Org:** `ptit_iot`

---

### Bước 2: Khởi động Bộ thu thập dữ liệu (MQTT Collector)
Mở cửa sổ Terminal / PowerShell mới:
```powershell
python ingestion\mqtt_collector.py
```
Collector sẽ kết nối tới MQTT Broker `broker.emqx.io:1883`, lắng nghe topic `iot/b23dccn522/sensor/raw`, validate dữ liệu và ghi vào InfluxDB theo thời gian thực.

---

### Bước 3: Phát dữ liệu cảm biến (Chọn 1 trong 2 cách)

#### Cách 3A: Dùng trình mô phỏng ESP32 trên Wokwi (Rất khuyến khích cho Demo)
1. Truy cập [Wokwi ESP32 Simulator](https://wokwi.com/projects/new/esp32).
2. Sao chép nội dung file [`firmware/diagram.json`](file:///firmware/diagram.json) vào tab **diagram.json** trên Wokwi.
3. Sao chép toàn bộ code từ file [`firmware/esp32_firmware.ino`](file:///firmware/esp32_firmware.ino) vào tab **sketch.ino** trên Wokwi.
4. Bấm nút **Play (Start Simulation)** trên Wokwi. ESP32 ảo sẽ tự kết nối Wi-Fi `Wokwi-GUEST` và phát dữ liệu MQTT thật lên Broker.

#### Cách 3B: Dùng bộ giả lập Virtual Sensor (Chạy trực tiếp trên máy tính)
Mở thêm một cửa sổ Terminal mới:
```powershell
python simulator\virtual_sensor.py
```
Bộ giả lập sẽ tự động sinh dữ liệu nhiệt độ, độ ẩm, ánh sáng liên tục kèm theo các kịch bản thực tế (bơm các mẫu Outlier > 80°C hoặc < -10°C, các mẫu bị thiếu Missing values) để kiểm thử bộ lọc.

---

### Bước 4: Chạy Pipeline Tiền xử lý dữ liệu
Sau khi hệ thống đã thu thập được dữ liệu, chạy script:
```powershell
python preprocessing\data_preprocessor.py
```
Script sẽ truy vấn dữ liệu từ `sensor_raw`, áp dụng thuật toán **IQR & Z-score**, khử outlier bằng Rolling Median, nội suy missing value, resampling theo chu kỳ 1 phút, trích xuất đặc trưng (`rolling_mean`, `delta`) và lưu vào measurement `sensor_processed`.

---

### Bước 5: Mở Dashboard Giám sát Real-time (Streamlit)
Mở giao diện Web Dashboard bằng lệnh:
```powershell
streamlit run dashboard\app.py
```
Dashboard sẽ tự động mở tại trình duyệt: `http://localhost:8501`.  
Tại đây bạn có thể:
- Xem các thẻ chỉ số thời gian thực (Nhiệt độ, Độ ẩm, Ánh sáng, Độ trễ Ingestion).
- Xem biểu đồ phát hiện điểm dị thường (Outlier) bằng các chấm đỏ.
- So sánh trực quan dữ liệu trước và sau khi làm sạch.
- Xem biểu đồ đặc trưng và chuẩn hóa Min-Max $[0, 1]$.
- Bấm nút **"Chạy Pipeline Tiền xử lý ngay"** trực tiếp trên Dashboard.

---

### Bước 6: Xuất Đồ thị Phân tích & Báo cáo Word
1. **Xuất đồ thị phân tích độ trễ:**
   ```powershell
   python analysis\latency_evaluator.py
   ```
2. **Tạo file Báo cáo Word chuẩn nộp bài (4–6 trang):**
   ```powershell
   python report\generate_report.py
   ```
   File báo cáo hoàn chỉnh sẽ được tạo tại thư mục `report/Bao_cao_Thuc_hanh_so_2_B23DCCN522.docx`.

---

## 📊 4. Thiết kế Schema Time-Series (InfluxDB)

* **Bucket:** `iot_telemetry`
* **Retention Policy:** 30 ngày (2,592,000 giây)
* **Measurement `sensor_raw`:**
  - **Tags:** `device_id`, `location`, `status` (`VALID` / `INVALID`)
  - **Fields:** `temperature`, `humidity`, `light`, `message_seq`, `latency_ingest_ms`
  - **Time:** Timestamp phát từ thiết bị
* **Measurement `sensor_processed`:**
  - **Tags:** `device_id`, `type` (`PREPROCESSED`)
  - **Fields:** `temperature_mean`, `temperature_min`, `temperature_max`, `temperature_rolling_mean`, `temperature_rolling_std`, `temperature_delta`, `temperature_normalized`, cùng các chỉ số cho `humidity` và `light`.

---


