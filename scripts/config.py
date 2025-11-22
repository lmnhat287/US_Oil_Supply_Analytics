import os

# Cấu hình kết nối Database
DB_CONFIG = {
    'user': 'root',
    'password': 'root',  # Phải khớp với docker-compose
    'host': 'localhost',
    'port': 3306,
    'database': 'oil_dw'
}

# Đường dẫn file (Dùng đường dẫn tương đối để code chạy được trên mọi máy)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

IMPORTS_CSV = os.path.join(DATA_DIR, 'data.csv')
PRODUCTION_CSV = os.path.join(DATA_DIR, 'OGORBcsv.csv')