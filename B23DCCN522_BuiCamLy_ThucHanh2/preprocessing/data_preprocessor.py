"""
==============================================================================
BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
Học viện Công nghệ Bưu chính Viễn thông (PTIT)
Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
Module: Data Preprocessing Pipeline (Quy trình tiền xử lý dữ liệu chuẩn hóa)
Các bước thực hiện theo yêu cầu đề bài:
  1. Đọc dữ liệu thô từ InfluxDB theo khung thời gian (Time range query)
  2. Xử lý giá trị thiếu (Missing values handling: interpolation / fill)
  3. Phát hiện và xử lý dị thường (Outlier detection: IQR & Z-score)
  4. Resampling theo cửa sổ thời gian (1 phút / 5 phút)
  5. Trích xuất đặc trưng (Feature engineering: rolling mean, std, delta)
  6. Chuẩn hóa dữ liệu (Min-Max Scaling về thang [0, 1])
  7. Lưu kết quả làm sạch vào measurement mới: sensor_processed
==============================================================================
"""

import time
import os
import sys
import yaml

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

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

RESAMPLE_RULE = config["preprocessing"]["resample_rule"]
Z_THRESHOLD = config["preprocessing"]["z_score_threshold"]
IQR_FACTOR = config["preprocessing"]["iqr_factor"]
ROLLING_WINDOW = config["preprocessing"]["rolling_window"]
LOOKBACK = config["preprocessing"]["time_range_lookback"]

def get_influx_client():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

def load_raw_data(lookback: str = LOOKBACK) -> pd.DataFrame:
    """
    Truy vấn dữ liệu thô từ InfluxDB bằng ngôn ngữ Flux và chuyển đổi sang DataFrame.
    """
    client = get_influx_client()
    query_api = client.query_api()

    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{lookback})
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_RAW}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''
    try:
        df = query_api.query_data_frame(flux_query)
        client.close()
    except Exception as e:
        print(f"❌ [LỖI TRUY VẤN INFLUXDB]: {e}")
        client.close()
        return pd.DataFrame()

    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if len(df) > 0 else pd.DataFrame()

    if df.empty or "_time" not in df.columns:
        return pd.DataFrame()

    # Chuẩn hóa cột thời gian làm index
    df["_time"] = pd.to_datetime(df["_time"])
    df.set_index("_time", inplace=True)
    df.sort_index(inplace=True)
    return df

def detect_outliers_iqr(series: pd.Series, factor: float = 1.5) -> pd.Series:
    """
    Phát hiện outlier bằng phương pháp IQR (Interquartile Range)
    Ngưỡng: [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
    """
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - factor * iqr
    upper_bound = q3 + factor * iqr
    return (series < lower_bound) | (series > upper_bound)

def detect_outliers_zscore(series: pd.Series, threshold: float = 3.0) -> pd.Series:
    """
    Phát hiện outlier bằng phương pháp Z-score (|Z| > threshold)
    """
    mean = series.mean()
    std = series.std()
    if std == 0 or np.isnan(std):
        return pd.Series(False, index=series.index)
    z_scores = (series - mean).abs() / std
    return z_scores > threshold

def preprocess_sensor_pipeline(df: pd.DataFrame):
    """
    Thực thi toàn bộ các bước tiền xử lý dữ liệu.
    """
    if df.empty or len(df) < 3:
        print("⚠️ Chưa đủ dữ liệu để chạy tiền xử lý (cần tối thiểu 3 mẫu).")
        return None, None

    features = ["temperature", "humidity", "light"]
    for f in features:
        if f not in df.columns:
            df[f] = np.nan

    cleaned_df = df.copy()

    # -------------------------------------------------------------
    # BƯỚC 1: XỬ LÝ MISSING VALUES (Giá trị thiếu)
    # -------------------------------------------------------------
    missing_summary = {}
    for col in features:
        missing_count = int(cleaned_df[col].isna().sum())
        missing_summary[col] = missing_count
        # Dùng phương pháp nội suy tuyến tính (Linear Interpolation)
        cleaned_df[col] = cleaned_df[col].interpolate(method="time").bfill().ffill()

    # -------------------------------------------------------------
    # BƯỚC 2: PHÁT HIỆN & XỬ LÝ OUTLIERS (IQR + Z-Score)
    # -------------------------------------------------------------
    outlier_summary = {}
    for col in features:
        outliers_iqr = detect_outliers_iqr(cleaned_df[col], factor=IQR_FACTOR)
        outliers_z = detect_outliers_zscore(cleaned_df[col], threshold=Z_THRESHOLD)
        combined_outliers = outliers_iqr | outliers_z

        outlier_count = int(combined_outliers.sum())
        outlier_summary[col] = outlier_count

        # Đánh dấu cột boolean nhận diện outlier
        cleaned_df[f"{col}_is_outlier"] = combined_outliers

        # Chiến lược thay thế: Dùng Rolling Median cửa sổ trượt để làm mịn điểm dị thường
        rolling_median = cleaned_df[col].rolling(window=5, min_periods=1, center=True).median()
        cleaned_df.loc[combined_outliers, col] = rolling_median[combined_outliers]

    # -------------------------------------------------------------
    # BƯỚC 3: RESAMPLING THEO CỬA SỔ THỜI GIAN (1 phút / 5 phút)
    # -------------------------------------------------------------
    resampled_df = cleaned_df[features].resample(RESAMPLE_RULE).agg({
        "temperature": ["mean", "min", "max"],
        "humidity": ["mean", "min", "max"],
        "light": ["mean", "min", "max"]
    })
    # Làm phẳng cột đa tầng (MultiIndex columns)
    resampled_df.columns = [f"{col}_{stat}" for col, stat in resampled_df.columns]
    resampled_df.dropna(how="all", inplace=True)

    # -------------------------------------------------------------
    # BƯỚC 4: FEATURE ENGINEERING (Trích xuất đặc trưng)
    # -------------------------------------------------------------
    for col in features:
        mean_col = f"{col}_mean"
        if mean_col in resampled_df.columns:
            # 1. Rolling Mean (Trung bình trượt)
            resampled_df[f"{col}_rolling_mean"] = resampled_df[mean_col].rolling(
                window=ROLLING_WINDOW, min_periods=1
            ).mean()
            # 2. Rolling Std (Độ lệch chuẩn trượt)
            resampled_df[f"{col}_rolling_std"] = resampled_df[mean_col].rolling(
                window=ROLLING_WINDOW, min_periods=1
            ).std().fillna(0)
            # 3. Delta (Tốc độ biến thiên so với chu kỳ trước)
            resampled_df[f"{col}_delta"] = resampled_df[mean_col].diff().fillna(0)

    # -------------------------------------------------------------
    # BƯỚC 5: CHUẨN HÓA DỮ LIỆU (Min-Max Normalization về thang [0, 1])
    # -------------------------------------------------------------
    scaler = MinMaxScaler()
    cols_to_scale = [f"{col}_mean" for col in features if f"{col}_mean" in resampled_df.columns]
    if len(cols_to_scale) > 0 and len(resampled_df) > 0:
        scaled_values = scaler.fit_transform(resampled_df[cols_to_scale])
        for idx, col in enumerate(cols_to_scale):
            resampled_df[f"{col.replace('_mean', '')}_normalized"] = scaled_values[:, idx]

    return cleaned_df, resampled_df, {
        "missing_summary": missing_summary,
        "outlier_summary": outlier_summary,
        "raw_points": len(df),
        "resampled_points": len(resampled_df)
    }

def save_processed_to_influx(resampled_df: pd.DataFrame, device_id: str = "ESP32_AGGREGATED"):
    """
    Ghi dữ liệu sau tiền xử lý vào measurement mới: sensor_processed
    """
    if resampled_df is None or resampled_df.empty:
        return 0

    client = get_influx_client()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    points = []
    for ts, row in resampled_df.iterrows():
        p = Point(MEASUREMENT_PROCESSED) \
            .tag("device_id", device_id) \
            .tag("type", "PREPROCESSED") \
            .time(ts, WritePrecision.NS)

        for col_name, val in row.items():
            if pd.notna(val):
                p.field(col_name, float(val))
        points.append(p)

    if points:
        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

    client.close()
    return len(points)

def run_pipeline():
    print("=" * 65)
    print("🔄 ĐANG CHẠY PIPELINE TIỀN XỬ LÝ DỮ LIỆU IoT...")
    print(f"⏱  Khoảng thời gian phân tích: {LOOKBACK} gần nhất")
    print("=" * 65)

    df_raw = load_raw_data(lookback=LOOKBACK)
    if df_raw.empty:
        print("⚠️ Chưa có dữ liệu thô trong InfluxDB! Hãy bật Collector và Sensor trước.")
        return

    print(f"📥 Đã tải {len(df_raw)} bản ghi từ '{MEASUREMENT_RAW}'.")

    cleaned_df, resampled_df, meta = preprocess_sensor_pipeline(df_raw)
    if resampled_df is None:
        return

    print("\n📊 BÁO CÁO LÀM SẠCH VÀ PHÁT HIỆN DỊ THƯỜNG:")
    print(f" - Giá trị thiếu (Missing) : {meta['missing_summary']}")
    print(f" - Dị thường (Outliers)   : {meta['outlier_summary']}")
    print(f" - Mẫu sau Resample ({RESAMPLE_RULE}): {meta['resampled_points']} bản ghi")

    saved_count = save_processed_to_influx(resampled_df)
    print(f"\n💾 Đã lưu thành công {saved_count} bản ghi vào '{MEASUREMENT_PROCESSED}'!")
    print("=" * 65)

if __name__ == "__main__":
    run_pipeline()
