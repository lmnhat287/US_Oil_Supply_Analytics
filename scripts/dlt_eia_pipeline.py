import os
import json
import dlt
import requests
from datetime import datetime

# Import các biến từ file config.py của bạn
from config import API_KEY, RAW_DIR, PROCESSED_DIR

# Định nghĩa thêm đường dẫn cho API Daily (nếu chưa có trong config)
API_RAW_DIR = os.path.join(RAW_DIR, 'eia_api_daily')
os.makedirs(API_RAW_DIR, exist_ok=True)

# Đường dẫn chuẩn trỏ vào file DuckDB trong thư mục Processed
DUCKDB_PATH = os.path.join(PROCESSED_DIR, 'zoomcamp_dw.duckdb')

# 1. Định nghĩa Nguồn dữ liệu (Source)
@dlt.resource(write_disposition="append")
def eia_oil_prices(api_key, start_date):
    url = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={api_key}&frequency=daily&data[0]=value&start={start_date}"
    
    print(f"🚀 Đang gọi API EIA Daily từ ngày {start_date}...")
    response = requests.get(url)
    response.raise_for_status() 
    data = response.json()
    
    # --- BƯỚC 1: LƯU RAW JSON ĐỂ BACKUP ---
    current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    raw_filename = os.path.join(API_RAW_DIR, f"eia_daily_prices_{current_time}.json")
    
    with open(raw_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print(f"📦 Đã lưu bản sao Raw JSON tại: {raw_filename}")

    # --- BƯỚC 2: YIELD CHO DLT XỬ LÝ ---
    if 'response' in data and 'data' in data['response']:
        yield data['response']['data']
    else:
        print("⚠️ API không trả về dữ liệu hợp lệ.")

if __name__ == "__main__":
    
    # 2. Khởi tạo Pipeline
    pipeline = dlt.pipeline(
        pipeline_name='eia_daily_ingestion',
        destination=dlt.destinations.duckdb(credentials=f"duckdb:///{DUCKDB_PATH}"),
        dataset_name='raw_layer' 
    )
    
    # 3. Chạy Pipeline (Lấy dữ liệu từ đầu năm 2024)
    print(f"🏗️ Bắt đầu xây dựng/cập nhật Data Warehouse tại: {DUCKDB_PATH}")
    load_info = pipeline.run(eia_oil_prices(api_key=API_KEY, start_date="2024-01-01"))
    
    print("✅ Hoàn thành dlt Pipeline!")
    print(load_info)