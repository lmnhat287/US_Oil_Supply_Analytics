import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG, PRODUCTION_CSV
from datetime import date
import sys

def load_production():
    # Tạo chuỗi kết nối
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600,connect_args={'connect_timeout': 60})

    print(f"🚀 Đang đọc file: {PRODUCTION_CSV}")
    try:
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
        
        df['ingestion_date'] = date.today()

        # Chọn cột
        cols = ['production_date', 'land_class', 'land_category', 'state', 'county',
                'fips_code', 'offshore_region', 'commodity', 'disposition_code',
                'disposition_desc', 'volume', 'ingestion_date']
        df = df[cols]

        print(f"📥 Đang nạp {len(df)} dòng vào bảng 'stg_federal_production'...")
        
        # Sử dụng connection transaction để đảm bảo an toàn
        with engine.begin() as connection:
            # 1. Nạp dữ liệu (replace sẽ drop bảng cũ và tạo mới)
            df.to_sql('stg_federal_production', connection, if_exists='replace', index=False,chunksize=500, method='multi')
            
            # 2. Thêm Primary Key ngay lập tức
            print("⚙️ Đang thêm Primary Key...")
            connection.execute(text("ALTER TABLE stg_federal_production ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;"))

        print("✅ Hoàn thành Production!")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        # QUAN TRỌNG: Raise lỗi để Airflow đánh dấu task là Failed
        sys.exit(1)

if __name__ == "__main__":
    load_production()