import requests
import pandas as pd
from config import DB_CONFIG, API_KEY
from sqlalchemy import create_engine, text
import json
from datetime import date
import sys

# API URL lấy giá dầu WTI theo tháng
URL = f"https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key={API_KEY}&frequency=monthly&data[0]=value&facets[series][]=RWTC&sort[0][column]=period&sort[0][direction]=desc"

def fetch_oil_prices():
    conn_str = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(conn_str)

    print(f"🚀 Đang gọi API lấy giá dầu WTI...")
    
    try:
        response = requests.get(URL)
        data = response.json()
        
        # 1. KIỂM TRA DỮ LIỆU TRẢ VỀ
        if 'response' not in data or 'data' not in data['response']:
            print("❌ Lỗi API: Cấu trúc phản hồi không đúng. Nội dung nhận được:")
            print(data) # In ra để xem lỗi là gì
            return
            
        raw_data = data['response']['data']
        if not raw_data:
            print("⚠️ Cảnh báo: API trả về danh sách rỗng. Vui lòng kiểm tra lại API Key.")
            return

        df = pd.DataFrame(raw_data)
        
        # Debug: In tên cột ra xem nó là gì
        print(f"ℹ️ Các cột nhận được từ API: {df.columns.tolist()}")

        # 2. CHUYỂN ĐỔI (Cách viết an toàn hơn)
        # Đổi tên cột trước
        df = df.rename(columns={'period': 'price_date', 'value': 'price_wti'})
        
        # Sau đó mới chọn cột (để tránh lỗi KeyError nếu chọn trước khi đổi)
        if 'price_date' in df.columns and 'price_wti' in df.columns:
            df = df[['price_date', 'price_wti']]
        else:
            print("❌ Lỗi: Không tìm thấy cột 'period' hoặc 'value' trong dữ liệu.")
            return
        
        # Làm sạch
        df['price_date'] = pd.to_datetime(df['price_date']).dt.date
        df['price_wti'] = df['price_wti'].astype(float)
        df['ingestion_date'] = date.today()

        print(f"📥 Đang nạp {len(df)} dòng vào bảng 'stg_oil_prices'...")

        # 3. NẠP VÀO MYSQL
        
        with engine.connect() as conn:
            df.to_sql('stg_oil_prices', engine, if_exists='replace', index=False)
            conn.execute(text("ALTER TABLE stg_oil_prices ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY FIRST;"))
            
        print("✅ Hoàn thành cập nhật Giá dầu!")

    except Exception as e:
        import traceback
        traceback.print_exc() # In chi tiết lỗi
        print(f"❌ Lỗi ngoại lệ: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_oil_prices()

