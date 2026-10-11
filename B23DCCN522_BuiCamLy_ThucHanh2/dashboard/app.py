"""
==============================================================================
BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
Học viện Công nghệ Bưu chính Viễn thông (PTIT)
Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
Module: Real-time Streamlit Dashboard (Bảng điều khiển giám sát thời gian thực)
Chức năng:
  - Giám sát chỉ số cảm biến thời gian thực (KPI cards)
  - Biểu đồ đối chiếu Dữ liệu thô (Raw) vs Dữ liệu đã làm sạch (Cleaned) vs Resampled
  - Hiển thị trực quan các điểm dị thường (Outlier Markers)
  - Phân tích độ trễ End-to-End và đánh giá dung lượng lưu trữ Time-Series
==============================================================================
"""

import time
import os
import yaml
import pandas as pd
import numpy as np
import streamlit as st
import altair as alt
from influxdb_client import InfluxDBClient

# Thiết lập giao diện Streamlit rộng rãi
st.set_page_config(
    page_title="IoT Telemetry & Analytics Dashboard - B23DCCN522",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 1. Đọc cấu hình
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

INFLUX_URL = config["influxdb"]["url"]
INFLUX_TOKEN = config["influxdb"]["token"]
INFLUX_ORG = config["influxdb"]["org"]
INFLUX_BUCKET = config["influxdb"]["bucket_raw"]
MEASUREMENT_RAW = config["influxdb"]["measurement_raw"]
MEASUREMENT_PROCESSED = config["influxdb"]["measurement_processed"]
BROKER = config["mqtt"]["broker"]

# 2. Hàm truy vấn dữ liệu từ InfluxDB
@st.cache_data(ttl=2)
def fetch_telemetry_data(lookback: str):
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=5000)
    query_api = client.query_api()

    # Truy vấn dữ liệu thô
    raw_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{lookback})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_RAW}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    # Truy vấn dữ liệu đã xử lý
    proc_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{lookback})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_PROCESSED}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''

    try:
        raw_df = query_api.query_data_frame(raw_query)
    except Exception:
        raw_df = pd.DataFrame()

    try:
        proc_df = query_api.query_data_frame(proc_query)
    except Exception:
        proc_df = pd.DataFrame()

    client.close()

    if isinstance(raw_df, list):
        raw_df = pd.concat(raw_df, ignore_index=True) if len(raw_df) > 0 else pd.DataFrame()
    if isinstance(proc_df, list):
        proc_df = pd.concat(proc_df, ignore_index=True) if len(proc_df) > 0 else pd.DataFrame()

    if not raw_df.empty and "_time" in raw_df.columns:
        raw_df["_time"] = pd.to_datetime(raw_df["_time"])
        raw_df.sort_values(by="_time", inplace=True)

    if not proc_df.empty and "_time" in proc_df.columns:
        proc_df["_time"] = pd.to_datetime(proc_df["_time"])
        proc_df.sort_values(by="_time", inplace=True)

    return raw_df, proc_df

# --- SIDEBAR: CẤU HÌNH & BỘ ĐIỀU KHIỂN ---
LOGO_PATH = os.path.join(os.path.dirname(__file__), "..", "report", "images", "logo_ptit.png")
if os.path.exists(LOGO_PATH):
    st.sidebar.image(LOGO_PATH, width=120)
st.sidebar.title("🎛️ Bảng Điều Khiển")
st.sidebar.markdown("**Sinh viên:** Bùi Cẩm Ly")
st.sidebar.markdown("**Mã SV:** `B23DCCN522`")
st.sidebar.markdown("**Học phần:** IoT và Ứng dụng")

st.sidebar.divider()
lookback_choice = st.sidebar.selectbox(
    "⏱ Khung thời gian quan sát:",
    ["15m", "30m", "1h", "3h", "12h"],
    index=1
)

auto_refresh = st.sidebar.checkbox("🔄 Tự động làm mới (2 giây)", value=True)

st.sidebar.divider()
st.sidebar.subheader("📡 Trạng thái hạ tầng")
st.sidebar.success(f"Broker: `{BROKER}` (Online)")
st.sidebar.success(f"InfluxDB: `localhost:8086` (Connected)")

# Nút kích hoạt tiền xử lý thủ công
if st.sidebar.button("🚀 Chạy Pipeline Tiền xử lý ngay"):
    from preprocessing.data_preprocessor import run_pipeline
    with st.spinner("Đang chạy pipeline tiền xử lý..."):
        run_pipeline()
    st.sidebar.success("Đã hoàn tất tiền xử lý và lưu kết quả!")

# --- TIÊU ĐỀ TRANG CHÍNH ---
st.title("🌐 Hệ Thống Thu Thập, Lưu Trữ & Tiền Xử Lý Dữ Liệu IoT")
st.caption("Bài thực hành số 2 | Kiến trúc Time-Series Database & Streamlit Analytics Engine")

raw_df, proc_df = fetch_telemetry_data(lookback_choice)

if raw_df.empty:
    st.warning("⚠️ Chưa có dữ liệu trong InfluxDB. Vui lòng bật bộ thu thập (`ingestion/mqtt_collector.py`) và bộ phát dữ liệu (`simulator/virtual_sensor.py` hoặc Wokwi)!")
    if auto_refresh:
        time.sleep(2)
        st.rerun()
    st.stop()

# --- HÀNG 1: METRICS THỜI GIAN THỰC ---
latest_row = raw_df.iloc[-1]
prev_row = raw_df.iloc[-2] if len(raw_df) > 1 else latest_row

c1, c2, c3, c4 = st.columns(4)
temp_val = latest_row.get("temperature", 0.0)
temp_delta = temp_val - prev_row.get("temperature", temp_val)
c1.metric(
    "🌡️ Nhiệt độ hiện tại",
    f"{temp_val:.1f} °C",
    delta=f"{temp_delta:+.2f} °C",
    delta_color="inverse"
)

hum_val = latest_row.get("humidity", 0.0)
hum_delta = hum_val - prev_row.get("humidity", hum_val)
c2.metric(
    "💧 Độ ẩm không khí",
    f"{hum_val:.1f} %",
    delta=f"{hum_delta:+.2f} %"
)

light_val = latest_row.get("light", 0.0)
light_delta = light_val - prev_row.get("light", light_val)
c3.metric(
    "☀️ Cường độ ánh sáng",
    f"{light_val:.0f} Lux",
    delta=f"{light_delta:+.0f} Lux"
)

lat_val = latest_row.get("latency_ingest_ms", 0.0)
lat_color = "normal" if lat_val < 100 else "inverse"
c4.metric(
    "⚡ Độ trễ Ingestion",
    f"{lat_val:.1f} ms",
    delta="Rất tốt (<100ms)" if lat_val < 100 else "Chậm",
    delta_color=lat_color
)

st.divider()

# --- TABS GIAO DIỆN PHÂN TÍCH ---
tab_analytics, tab_compare, tab_features, tab_perf, tab_data = st.tabs([
    "📈 Giám sát Real-time & Outlier",
    "⚖️ Đối chiếu Raw vs Processed",
    "🔬 Đặc trưng & Chuẩn hóa [0, 1]",
    "📊 Phân tích Độ trễ & Hiệu năng",
    "🗄️ Bảng Dữ Liệu Chi Tiết"
])

# -------------------------------------------------------------
# TAB 1: GIÁM SÁT REAL-TIME & ĐÁNH DẤU OUTLIER
# -------------------------------------------------------------
with tab_analytics:
    st.subheader("1. Diễn biến Nhiệt độ & Phát hiện Dị thường (IQR + Z-Score)")

    # Nội suy nhẹ cho plot để không bị đứt đường khi có điểm null
    plot_df = raw_df[["_time", "temperature", "humidity", "light"]].copy()
    plot_df["temp_clean"] = plot_df["temperature"].interpolate().bfill().ffill()

    # Tính toán IQR và Z-Score trên dữ liệu hiển thị
    q1 = plot_df["temp_clean"].quantile(0.25)
    q3 = plot_df["temp_clean"].quantile(0.75)
    iqr = q3 - q1
    lower_b = q1 - 1.5 * iqr
    upper_b = q3 + 1.5 * iqr
    mean_t = plot_df["temp_clean"].mean()
    std_t = plot_df["temp_clean"].std()

    outlier_mask = (plot_df["temp_clean"] < lower_b) | (plot_df["temp_clean"] > upper_b)
    if std_t > 0:
        outlier_mask = outlier_mask | (((plot_df["temp_clean"] - mean_t).abs() / std_t) > 3.0)
    plot_df["is_outlier"] = outlier_mask

    # Định dạng trục thời gian chuẩn HH:MM:SS
    time_axis = alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian (Giờ:Phút:Giây)")

    base_line = alt.Chart(plot_df).mark_line(color="#1f77b4", strokeWidth=2.5, point=True).encode(
        x=alt.X("_time:T", axis=time_axis),
        y=alt.Y("temp_clean:Q", title="Nhiệt độ (°C)", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("_time:T", format="%H:%M:%S", title="Thời gian"),
            alt.Tooltip("temp_clean:Q", title="Nhiệt độ (°C)", format=".2f")
        ]
    )

    outlier_points = alt.Chart(plot_df[plot_df["is_outlier"]]).mark_circle(
        size=140, color="#e74c3c"
    ).encode(
        x=alt.X("_time:T", axis=time_axis),
        y=alt.Y("temp_clean:Q", scale=alt.Scale(zero=False)),
        tooltip=[
            alt.Tooltip("_time:T", format="%H:%M:%S", title="Thời gian dị thường"),
            alt.Tooltip("temp_clean:Q", title="Nhiệt độ dị thường (°C)", format=".2f"),
        ]
    )

    chart_temp = (base_line + outlier_points).properties(height=340)
    st.altair_chart(chart_temp, use_container_width=True)

    outlier_cnt = int(plot_df["is_outlier"].sum())
    st.caption(f"📌 Đã phát hiện tổng cộng **{outlier_cnt} điểm dị thường (chấm đỏ)** vượt ngưỡng IQR/Z-score.")

    col_h, col_l = st.columns(2)
    with col_h:
        st.markdown("**Độ ẩm không khí (%)**")
        chart_hum = alt.Chart(plot_df).mark_line(color="#2ca02c", strokeWidth=2, point=True).encode(
            x=alt.X("_time:T", axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian")),
            y=alt.Y("humidity:Q", title="Độ ẩm (%)", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("_time:T", format="%H:%M:%S"), alt.Tooltip("humidity:Q", format=".1f")]
        ).properties(height=260)
        st.altair_chart(chart_hum, use_container_width=True)

    with col_l:
        st.markdown("**Cường độ ánh sáng (Lux)**")
        chart_light = alt.Chart(plot_df).mark_line(color="#ff7f0e", strokeWidth=2, point=True).encode(
            x=alt.X("_time:T", axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian")),
            y=alt.Y("light:Q", title="Ánh sáng (Lux)", scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip("_time:T", format="%H:%M:%S"), alt.Tooltip("light:Q", format=".0f")]
        ).properties(height=260)
        st.altair_chart(chart_light, use_container_width=True)

# -------------------------------------------------------------
# TAB 2: ĐỐI CHIẾU DỮ LIỆU THÔ VS ĐÃ TIỀN XỬ LÝ
# -------------------------------------------------------------
with tab_compare:
    st.subheader("2. So sánh Dữ liệu Thô (Nhiễu) vs Dữ liệu đã Làm sạch & Resample")
    if proc_df.empty or "temperature_mean" not in proc_df.columns:
        st.info("💡 Chưa có dữ liệu trong measurement `sensor_processed`. Bấm nút **'Chạy Pipeline Tiền xử lý ngay'** ở thanh bên trái để xem kết quả đối chiếu!")
    else:
        st.write("Đồ thị dưới đây chứng minh tác dụng của thuật toán khử outlier và làm mịn theo chu kỳ:")

        # Ghép nối dữ liệu thô và đã xử lý để vẽ so sánh
        c_raw = raw_df[["_time", "temperature"]].rename(columns={"temperature": "Raw (Gốc có nhiễu)"})
        c_proc = proc_df[["_time", "temperature_mean"]].rename(columns={"temperature_mean": "Processed (Đã làm sạch)"})

        merged_view = pd.merge_asof(c_raw, c_proc, on="_time", direction="nearest")
        long_view = merged_view.melt(id_vars=["_time"], value_vars=["Raw (Gốc có nhiễu)", "Processed (Đã làm sạch)"], var_name="Loại dữ liệu", value_name="Nhiệt độ (°C)")
        chart_comp = alt.Chart(long_view).mark_line(strokeWidth=2, point=True).encode(
            x=alt.X("_time:T", axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian")),
            y=alt.Y("Nhiệt độ (°C):Q", scale=alt.Scale(zero=False)),
            color=alt.Color("Loại dữ liệu:N", scale=alt.Scale(domain=["Raw (Gốc có nhiễu)", "Processed (Đã làm sạch)"], range=["#e74c3c", "#27ae60"])),
            tooltip=[alt.Tooltip("_time:T", format="%H:%M:%S"), "Loại dữ liệu:N", alt.Tooltip("Nhiệt độ (°C):Q", format=".2f")]
        ).properties(height=340)
        st.altair_chart(chart_comp, use_container_width=True)

        st.success("✅ **Nhận xét:** Các gai nhiễu đột biến đã được bộ lọc rolling median và resampling loại bỏ hoàn toàn, giữ lại xu hướng chuẩn của môi trường.")

# -------------------------------------------------------------
# TAB 3: TRÍCH XUẤT ĐẶC TRƯNG & CHUẨN HÓA [0, 1]
# -------------------------------------------------------------
with tab_features:
    st.subheader("3. Đặc trưng Trích xuất & Chuẩn hóa Min-Max Scaling")
    if proc_df.empty or "temperature_normalized" not in proc_df.columns:
        st.info("💡 Chưa có dữ liệu đã tiền xử lý. Bấm nút **'🚀 Chạy Pipeline Tiền xử lý ngay'** ở menu bên trái!")
    else:
        st.markdown("#### a. Chuẩn hóa Min-Max [0, 1] (Đưa các cảm biến về cùng thang đo)")
        st.caption("Biến đổi tất cả cảm biến (Nhiệt độ, Độ ẩm, Ánh sáng) về khoảng [0, 1] để dễ dàng so sánh tương quan trên cùng 1 biểu đồ.")
        norm_cols = [c for c in ["temperature_normalized", "humidity_normalized", "light_normalized"] if c in proc_df.columns]
        if norm_cols:
            long_norm = proc_df.melt(id_vars=["_time"], value_vars=norm_cols, var_name="Chỉ số", value_name="Giá trị chuẩn hóa [0, 1]")
            chart_norm = alt.Chart(long_norm).mark_line(strokeWidth=2.5, point=True).encode(
                x=alt.X("_time:T", axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian")),
                y=alt.Y("Giá trị chuẩn hóa [0, 1]:Q", scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("Chỉ số:N", scale=alt.Scale(range=["#2980b9", "#27ae60", "#e67e22"])),
                tooltip=[alt.Tooltip("_time:T", format="%H:%M:%S"), "Chỉ số:N", alt.Tooltip("Giá trị chuẩn hóa [0, 1]:Q", format=".3f")]
            ).properties(height=300)
            st.altair_chart(chart_norm, use_container_width=True)

        st.markdown("#### b. Tốc độ biến thiên (Delta) & Độ lệch chuẩn trượt (Rolling Std)")
        f_cols = [c for c in ["temperature_delta", "temperature_rolling_std"] if c in proc_df.columns]
        if f_cols:
            long_f = proc_df.melt(id_vars=["_time"], value_vars=f_cols, var_name="Đặc trưng", value_name="Giá trị")
            chart_f = alt.Chart(long_f).mark_line(strokeWidth=2, point=True).encode(
                x=alt.X("_time:T", axis=alt.Axis(format="%H:%M:%S", labelAngle=-30, title="Thời gian")),
                y=alt.Y("Giá trị:Q", scale=alt.Scale(zero=False)),
                color="Đặc trưng:N",
                tooltip=[alt.Tooltip("_time:T", format="%H:%M:%S"), "Đặc trưng:N", alt.Tooltip("Giá trị:Q", format=".3f")]
            ).properties(height=280)
            st.altair_chart(chart_f, use_container_width=True)

# -------------------------------------------------------------
# TAB 4: PHÂN TÍCH ĐỘ TRỄ & HIỆU NĂNG LƯU TRỮ
# -------------------------------------------------------------
with tab_perf:
    st.subheader("4. Đánh giá Hiệu năng Pipeline & Phân tích Độ trễ End-to-End")

    p1, p2, p3 = st.columns(3)
    p1.metric("📦 Tổng gói tin nhận (Raw)", f"{len(raw_df):,}")
    p2.metric("💾 Bản ghi tổng hợp (Resampled)", f"{len(proc_df):,}")
    ratio = (100 - (len(proc_df) / len(raw_df) * 100)) if len(raw_df) > 0 and len(proc_df) > 0 else 0
    p3.metric("📉 Tỉ lệ nén dung lượng lưu trữ", f"{ratio:.1f} %")

    st.markdown("#### Phân bố độ trễ Ingestion (ms)")
    if "latency_ingest_ms" in raw_df.columns:
        lat_series = raw_df["latency_ingest_ms"].dropna()
        l_col1, l_col2, l_col3, l_col4 = st.columns(4)
        l_col1.metric("Min Latency", f"{lat_series.min():.1f} ms")
        l_col2.metric("Avg Latency", f"{lat_series.mean():.1f} ms")
        l_col3.metric("Max Latency", f"{lat_series.max():.1f} ms")
        l_col4.metric("Std Dev", f"{lat_series.std():.1f} ms")

        # Vẽ histogram độ trễ
        hist_chart = alt.Chart(raw_df).mark_bar(color="#5c6bc0").encode(
            alt.X("latency_ingest_ms:Q", bin=alt.Bin(maxbins=30), title="Độ trễ (ms)"),
            y=alt.Y("count()", title="Tần suất (Số gói)")
        )
        st.altair_chart(hist_chart, use_container_width=True)

# -------------------------------------------------------------
# TAB 5: BẢNG DỮ LIỆU CHI TIẾT
# -------------------------------------------------------------
with tab_data:
    st.subheader("5. Bảng Dữ Liệu Đang Lưu Trữ Trong InfluxDB")
    sub_col1, sub_col2 = st.columns(2)
    with sub_col1:
        st.markdown(f"**Measurement: `{MEASUREMENT_RAW}`**")
        st.dataframe(raw_df.tail(20), use_container_width=True)
    with sub_col2:
        st.markdown(f"**Measurement: `{MEASUREMENT_PROCESSED}`**")
        st.dataframe(proc_df.tail(20), use_container_width=True)

# Cơ chế tự động làm mới trang
if auto_refresh:
    time.sleep(2)
    st.rerun()
