import pandas as pd
from sqlalchemy import create_engine, text
from config import DB_CONFIG, IMPORTS_CSV
from datetime import date
import sys # <--- Thêm thư viện sys

def load_imports():
    # Tạo kết nối
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600,connect_args={'connect_timeout': 60})

    print(f"🚀 Đang đọc file: {IMPORTS_CSV}")
    try:
        df = pd.read_csv(IMPORTS_CSV)
        
        # Mapping cột
        df.rename(columns={
            'year': 'year', 'month': 'month',
            'originName': 'origin_name', 'originTypeName': 'origin_type',
            'destinationName': 'destination_name', 'destinationTypeName': 'destination_type',
            'gradeName': 'grade_name', 'quantity': 'quantity_thousand_bbl'
        }, inplace=True)

        # Clean dữ liệu
        if df['quantity_thousand_bbl'].dtype == 'object':
            df['quantity_thousand_bbl'] = df['quantity_thousand_bbl'].astype(str).str.replace(',', '').astype(int)
            
        df['ingestion_date'] = date.today()

        # Chọn cột cần thiết
        cols = ['year', 'month', 'origin_name', 'origin_type', 'destination_name', 
                'destination_type', 'grade_name', 'quantity_thousand_bbl', 'ingestion_date']
        df = df[cols]

        print(f"📥 Đang nạp {len(df)} dòng vào bảng 'stg_crude_oil_imports'...")
        
        # Dùng transaction để đảm bảo an toàn khi ALTER TABLE
        with engine.begin() as connection:
            # 1. Nạp dữ liệu (replace sẽ drop table cũ và create table mới dựa trên DataFrame)
            # Lưu ý: Lúc này bảng chưa có Primary Key
            df.to_sql('stg_crude_oil_imports', connection, if_exists='replace', index=False,chunksize=500, method='multi')
            
            # 2. Thêm Primary Key ngay sau khi tạo bảng
            print("⚙️ Đang thêm Primary Key...")
            connection.execute(text("ALTER TABLE stg_crude_oil_imports ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;"))
            
        print("✅ Hoàn thành Import!")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        # QUAN TRỌNG: Phải raise lỗi để Airflow biết task failed
        sys.exit(1)

if __name__ == "__main__":
    load_imports()