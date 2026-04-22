import requests
import pandas as pd
from config import API_KEY, PROCESSED_DIR, RAW_DIR
import os
import json
from datetime import datetime
import sys

URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={API_KEY}&frequency=monthly&data[0]=value&facets[series][]=RWTC&sort[0][column]=period&sort[0][direction]=desc"

def fetch_oil_prices():
    API_RAW_DIR = os.path.join(RAW_DIR, 'eia_api_monthly')
    os.makedirs(API_RAW_DIR, exist_ok=True)
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    parquet_path = os.path.join(PROCESSED_DIR, 'stg_oil_prices.parquet')

    print(f"🚀 Đang gọi API lấy giá dầu WTI...")
    try:
        response = requests.get(URL)
        response.raise_for_status()
        data = response.json()
        
        # 1. LƯU BẢN SAO RAW JSON
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_filename = os.path.join(API_RAW_DIR, f"prices_monthly_{current_time}.json")
        with open(raw_filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print(f"📦 Đã lưu bản sao Raw JSON tại: {raw_filename}")

        if 'response' not in data or 'data' not in data['response']:
            print("❌ Lỗi API: Cấu trúc phản hồi không đúng.")
            return
            
        raw_data = data['response']['data']
        if not raw_data:
            print("⚠️ Cảnh báo: API trả về danh sách rỗng.")
            return

        df = pd.DataFrame(raw_data)
        
        # 2. CHUYỂN ĐỔI VÀ LÀM SẠCH
        df = df.rename(columns={'period': 'price_date', 'value': 'price_wti'})
        if 'price_date' in df.columns and 'price_wti' in df.columns:
            df = df[['price_date', 'price_wti']]
        else:
            print("❌ Lỗi: Không tìm thấy cột 'period' hoặc 'value' trong dữ liệu.")
            return
            
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['price_wti'] = df['price_wti'].astype(float)
        df['ingestion_date'] = pd.to_datetime(datetime.today())

        # 3. NẠP VÀO PARQUET TRONG THƯ MỤC PROCESSED
        print(f"📥 Đang xuất {len(df)} dòng thành file Parquet tại '{parquet_path}'...")
        df.to_parquet(parquet_path, engine='pyarrow', index=False)
        print("✅ Hoàn thành cập nhật Giá dầu sang Parquet!")

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"❌ Lỗi ngoại lệ: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_oil_prices()