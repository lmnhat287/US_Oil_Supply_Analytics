import pandas as pd
import requests
from sqlalchemy import create_engine, text
from config import DB_CONFIG, API_KEY
import sys

# Cấu hình các chỉ số cần lấy (Series ID lấy từ EIA)
EIA_SERIES = {
    'stg_oil_stocks': {
        'series_id': 'PET.WCRSTUS1.W', 
        'name': 'Commercial Crude Oil Stocks',
        'desc': 'Tồn kho dầu thô thương mại (Thùng)'
    },
    'stg_refinery_inputs': {
        'series_id': 'PET.WCRRIUS2.W',
        'name': 'Refiner Net Input',
        'desc': 'Đầu vào nhà máy lọc dầu (Thùng/Ngày)'
    },
    'stg_oil_exports': {
        'series_id': 'PET.MCREXUS1.M',
        'name': 'Crude Oil Exports',
        'desc': 'Xuất khẩu dầu thô (Thùng)'
    }
}

def fetch_and_load_eia_data():
    # Tạo kết nối DB
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str, pool_pre_ping=True, pool_recycle=3600)

    for table_name, config in EIA_SERIES.items():
        series_id = config['series_id']
        print(f"🚀 Đang lấy dữ liệu: {config['name']} ({series_id})...")
        
        # Gọi API EIA v2
        url = f"https://api.eia.gov/v2/seriesid/{series_id}?api_key={API_KEY}"
        
        try:
            response = requests.get(url, timeout=30)
            data = response.json()
            
            if 'response' not in data or 'data' not in data['response']:
                print(f"⚠️ Không có dữ liệu trả về cho {series_id}")
                continue

            # Chuyển thành DataFrame
            records = data['response']['data']
            df = pd.DataFrame(records)
            
            # Làm sạch dữ liệu cơ bản
            # Giữ lại cột ngày và giá trị
            df = df[['period', 'value', 'units']]
            df.rename(columns={'period': 'date', 'value': 'value', 'units': 'unit'}, inplace=True)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['metric_name'] = config['name']
            
            # Nạp vào Database
            print(f"📥 Đang nạp {len(df)} dòng vào bảng '{table_name}'...")
            
            with engine.begin() as connection:
                df.to_sql(table_name, connection, if_exists='replace', index=False)
                # Thêm Primary Key
                connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;"))
            
            print(f"✅ Hoàn thành bảng {table_name}!")

        except Exception as e:
            print(f"❌ Lỗi khi xử lý {table_name}: {e}")
            sys.exit(1) # Báo lỗi cho Airflow

if __name__ == "__main__":
    fetch_and_load_eia_data()