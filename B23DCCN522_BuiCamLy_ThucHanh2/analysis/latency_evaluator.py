"""
==============================================================================
BÀI THỰC HÀNH SỐ 2: THU THẬP, LƯU TRỮ VÀ TIỀN XỬ LÝ DỮ LIỆU IoT
Học viện Công nghệ Bưu chính Viễn thông (PTIT)
Sinh viên: Bùi Cẩm Ly - Mã SV: B23DCCN522
Module: Latency & Performance Evaluator (Phân tích độ trễ & xuất biểu đồ báo cáo)
Chức năng:
  - Tính toán các chỉ số thống kê độ trễ End-to-End (Min, Max, Mean, P95, P99)
  - Vẽ và lưu các biểu đồ thực nghiệm (PNG) phục vụ chèn vào Báo cáo Word:
    1. latency_distribution.png
    2. raw_vs_processed_comparison.png
==============================================================================
"""

import os
import sys
import yaml

if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from influxdb_client import InfluxDBClient

# Thiết lập style đồ thị cho đẹp mắt
plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.yaml")
with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

INFLUX_URL = config["influxdb"]["url"]
INFLUX_TOKEN = config["influxdb"]["token"]
INFLUX_ORG = config["influxdb"]["org"]
INFLUX_BUCKET = config["influxdb"]["bucket_raw"]
MEASUREMENT_RAW = config["influxdb"]["measurement_raw"]
MEASUREMENT_PROCESSED = config["influxdb"]["measurement_processed"]

OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "report", "images"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_data():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    query_api = client.query_api()

    raw_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -3h)
      |> filter(fn: (r) => r["_measurement"] == "{MEASUREMENT_RAW}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"])
    '''
    try:
        raw_df = query_api.query_data_frame(raw_query)
    except Exception as e:
        print(f"Lỗi query InfluxDB: {e}")
        raw_df = pd.DataFrame()

    client.close()
    if isinstance(raw_df, list):
        raw_df = pd.concat(raw_df, ignore_index=True) if len(raw_df) > 0 else pd.DataFrame()
    return raw_df

def evaluate_and_plot():
    print("=" * 65)
    print("📈 ĐANG PHÂN TÍCH ĐỘ TRỄ & XUẤT BIỂU ĐỒ BÁO CÁO...")
    print("=" * 65)

    df = fetch_data()
    if df.empty or "latency_ingest_ms" not in df.columns:
        print("⚠️ Chưa có đủ dữ liệu độ trễ để phân tích.")
        return None

    lat = df["latency_ingest_ms"].dropna()
    metrics = {
        "count": len(lat),
        "min": lat.min(),
        "max": lat.max(),
        "mean": lat.mean(),
        "std": lat.std(),
        "p50": lat.median(),
        "p95": lat.quantile(0.95),
        "p99": lat.quantile(0.99)
    }

    print("📊 KẾT QUẢ THỐNG KÊ ĐỘ TRỄ END-TO-END:")
    print(f" - Tổng số mẫu       : {metrics['count']}")
    print(f" - Min Latency       : {metrics['min']:.2f} ms")
    print(f" - Mean (Trung bình) : {metrics['mean']:.2f} ms")
    print(f" - Median (P50)      : {metrics['p50']:.2f} ms")
    print(f" - 95th Percentile   : {metrics['p95']:.2f} ms")
    print(f" - 99th Percentile   : {metrics['p99']:.2f} ms")
    print(f" - Max Latency       : {metrics['max']:.2f} ms")

    # 1. Vẽ đồ thị phân bố độ trễ (Histogram + Boxplot)
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    ax1.hist(lat, bins=30, color="#3498db", edgecolor="black", alpha=0.7)
    ax1.axvline(metrics["mean"], color="red", linestyle="dashed", linewidth=1.5, label=f"Mean: {metrics['mean']:.1f}ms")
    ax1.axvline(metrics["p95"], color="orange", linestyle="dotted", linewidth=1.5, label=f"P95: {metrics['p95']:.1f}ms")
    ax1.set_title("Phan bo Do tre Ingestion (End-to-End Latency)", fontsize=13, fontweight='bold')
    ax1.set_xlabel("Do tre (ms)", fontsize=11)
    ax1.set_ylabel("Tan suat (So goi)", fontsize=11)
    ax1.legend()

    ax2.boxplot(lat, orientation='vertical', patch_artist=True,
                boxprops=dict(facecolor="#a8d5e5", color="#1b4f72"),
                medianprops=dict(color="red", linewidth=2))
    ax2.set_title("Bieu do Boxplot Do tre", fontsize=13, fontweight='bold')
    ax2.set_ylabel("Do tre (ms)", fontsize=11)

    lat_img_path = os.path.join(OUTPUT_DIR, "latency_distribution.png")
    plt.tight_layout()
    plt.savefig(lat_img_path, dpi=200)
    plt.close()
    print(f"✅ Đã lưu biểu đồ: {lat_img_path}")

    # 2. Vẽ đồ thị so sánh Dữ liệu Nhiệt độ thô & Đánh dấu Outlier
    if "temperature" in df.columns:
        plt.figure(figsize=(12, 5))
        df["_time"] = pd.to_datetime(df["_time"])
        plt.plot(df["_time"], df["temperature"], label="Nhiet do tho (Raw)", color="#2980b9", alpha=0.8, linewidth=1.5)

        q1 = df["temperature"].quantile(0.25)
        q3 = df["temperature"].quantile(0.75)
        iqr = q3 - q1
        outliers = df[(df["temperature"] < q1 - 1.5 * iqr) | (df["temperature"] > q3 + 1.5 * iqr)]
        if not outliers.empty:
            plt.scatter(outliers["_time"], outliers["temperature"], color="red", s=60, zorder=5, label=f"Diem di thuong Outlier ({len(outliers)} diem)")

        plt.title("Dien bien Nhiet do va Phat hien Di thuong (IQR Method)", fontsize=13, fontweight='bold')
        plt.xlabel("Thoi gian", fontsize=11)
        plt.ylabel("Nhiet do (°C)", fontsize=11)
        plt.legend()
        plt.tight_layout()

        temp_img_path = os.path.join(OUTPUT_DIR, "raw_temperature_outliers.png")
        plt.savefig(temp_img_path, dpi=200)
        plt.close()
        print(f"✅ Đã lưu biểu đồ: {temp_img_path}")

    return metrics

if __name__ == "__main__":
    evaluate_and_plot()
