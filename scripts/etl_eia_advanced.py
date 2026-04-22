import pandas as pd
import requests
from config import API_KEY, PROCESSED_DIR, RAW_DIR
import sys
import os
import json
from datetime import datetime

EIA_SERIES = {
    'stg_oil_stocks': {'series_id': 'PET.WCRSTUS1.W', 'name': 'Commercial Crude Oil Stocks'},
    'stg_refinery_inputs': {'series_id': 'PET.WCRRIUS2.W', 'name': 'Refiner Net Input'},
    'stg_oil_exports': {'series_id': 'PET.MCREXUS1.M', 'name': 'Crude Oil Exports'}
}

def fetch_and_load_eia_data():
    API_RAW_DIR = os.path.join(RAW_DIR, 'eia_api_advanced')
    os.makedirs(API_RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    for table_name, config in EIA_SERIES.items():
        series_id = config['series_id']
        print(f"🚀 Đang lấy dữ liệu: {config['name']} ({series_id})...")
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={API_KEY}"
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # LƯU BẢN SAO RAW JSON
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
            raw_filename = os.path.join(API_RAW_DIR, f"{table_name}_{current_time}.json")
            with open(raw_filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"📦 Đã lưu Raw JSON: {raw_filename}")

            if 'response' not in data or 'data' not in data['response']:
                print(f"⚠️ Không có dữ liệu trả về cho {series_id}")
                continue

            records = data['response']['data']
            df = pd.DataFrame(records)
            
            # LÀM SẠCH VÀ LƯU PARQUET VÀO PROCESSED
            df = df[['period', 'value', 'units']]
            df.rename(columns={'period': 'date', 'value': 'value', 'units': 'unit'}, inplace=True)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['metric_name'] = config['name']
            df['ingestion_date'] = pd.to_datetime(datetime.today())

            parquet_path = os.path.join(PROCESSED_DIR, f'{table_name}.parquet')
            print(f"📥 Nạp {len(df)} dòng vào Parquet '{parquet_path}'...")
            df.to_parquet(parquet_path, engine='pyarrow', index=False)
            print(f"✅ Hoàn thành {table_name}!")

        except Exception as e:
            print(f"❌ Lỗi khi xử lý {table_name}: {e}")
            sys.exit(1)

if __name__ == "__main__":
    fetch_and_load_eia_data()