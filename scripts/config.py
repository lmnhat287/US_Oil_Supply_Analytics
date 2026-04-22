import os

# 1. Lấy thư mục gốc của dự án
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
API_KEY = 'UQu9LfqVCIUQwRaicsh3cJJ9Dq6znVnpHWxlT1vL'

# 2. Quy hoạch Data Lake
DATA_LAKE_DIR = os.path.join(BASE_DIR, 'data_lake')
RAW_DIR = os.path.join(DATA_LAKE_DIR, 'raw')
PROCESSED_DIR = os.path.join(DATA_LAKE_DIR, 'processed')

# 3. Đường dẫn cụ thể cho các file gốc (Để script đọc)
IMPORTS_CSV = os.path.join(RAW_DIR, 'data.csv')
PRODUCTION_CSV = os.path.join(RAW_DIR, 'OGORBcsv.csv')

# 4. Đường dẫn xuất file Parquet (Để script ghi)
IMPORTS_PARQUET = os.path.join(PROCESSED_DIR, 'stg_crude_oil_imports.parquet')
PRODUCTION_PARQUET = os.path.join(PROCESSED_DIR, 'stg_federal_production.parquet')
PRICES_PARQUET = os.path.join(PROCESSED_DIR, 'stg_oil_prices.parquet')
EIA_ADVANCED_PARQUET = os.path.join(PROCESSED_DIR, 'stg_eia_advanced.parquet')