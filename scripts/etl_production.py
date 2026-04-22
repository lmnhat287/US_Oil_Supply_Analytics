import pandas as pd
from datetime import date
import sys
import os

# Import các đường dẫn từ config
from config import PRODUCTION_CSV, PRODUCTION_PARQUET, PROCESSED_DIR

def load_production():
    print(f"🚀 Đang đọc file gốc từ: {PRODUCTION_CSV}")
    try:
        if not os.path.exists(PRODUCTION_CSV):
            raise FileNotFoundError(f"Không tìm thấy file {PRODUCTION_CSV}. Vui lòng kiểm tra thư mục raw.")

        df = pd.read_csv(PRODUCTION_CSV)
        
        # Mapping cột
        df.rename(columns={
            'Production Date': 'production_date', 'Land Class': 'land_class',
            'Land Category': 'land_category', 'State': 'state',
            'County': 'county', 'FIPS Code': 'fips_code',
            'Offshore Region': 'offshore_region', 'Commodity': 'commodity',
            'Disposition Code': 'disposition_code', 'Disposition Description': 'disposition_desc',
            'Volume': 'volume'
        }, inplace=True)

        # Clean Date
        df['production_date'] = pd.to_datetime(df['production_date'], format='%m/%d/%Y').dt.date
        
        # Clean Volume
        df['volume'] = df['volume'].astype(str).str.replace(',', '').replace('nan', '0').astype(float).astype('Int64')
        
        # Clean FIPS & Codes
        df['fips_code'] = df['fips_code'].astype(str).str.replace(r'\.0$', '', regex=True).replace('nan', None)
        df['disposition_code'] = df['disposition_code'].astype(str)
        
        df['ingestion_date'] = pd.to_datetime(date.today())

        # Chọn cột
        cols = ['production_date', 'land_class', 'land_category', 'state', 'county',
                'fips_code', 'offshore_region', 'commodity', 'disposition_code',
                'disposition_desc', 'volume', 'ingestion_date']
        df = df[cols]

        # Đảm bảo thư mục processed đã tồn tại trước khi lưu
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        print(f"📥 Đang xuất {len(df)} dòng thành file Parquet...")
        
        # Lưu thành file Parquet
        df.to_parquet(PRODUCTION_PARQUET, engine='pyarrow', index=False)

        print(f"✅ Hoàn thành! File đã được lưu tại: {PRODUCTION_PARQUET}")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load_production()