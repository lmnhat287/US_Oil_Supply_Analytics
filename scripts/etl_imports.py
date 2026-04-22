import pandas as pd
from datetime import date
import sys
import os

# Import các đường dẫn từ config
from config import IMPORTS_CSV, IMPORTS_PARQUET, PROCESSED_DIR

def load_imports():
    print(f"🚀 Đang đọc file gốc từ: {IMPORTS_CSV}")
    try:
        # Kiểm tra xem file raw có tồn tại không
        if not os.path.exists(IMPORTS_CSV):
            raise FileNotFoundError(f"Không tìm thấy file {IMPORTS_CSV}. Vui lòng kiểm tra thư mục raw.")

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
            
        df['ingestion_date'] = pd.to_datetime(date.today())

        # Chọn cột cần thiết
        cols = ['year', 'month', 'origin_name', 'origin_type', 'destination_name', 
                'destination_type', 'grade_name', 'quantity_thousand_bbl', 'ingestion_date']
        df = df[cols]

        # Đảm bảo thư mục processed đã tồn tại trước khi lưu
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        print(f"📥 Đang xuất {len(df)} dòng thành file Parquet...")
        
        # Lưu thành file Parquet vào thư mục Processed
        df.to_parquet(IMPORTS_PARQUET, engine='pyarrow', index=False)
            
        print(f"✅ Hoàn thành! File đã được lưu tại: {IMPORTS_PARQUET}")

    except Exception as e:
        print(f"❌ Lỗi nghiêm trọng: {e}")
        sys.exit(1)

if __name__ == "__main__":
    load_imports()