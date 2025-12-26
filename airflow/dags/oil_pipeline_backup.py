from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. CẤU HÌNH CHUNG
default_args = {
    'owner': 'minh_nhat',
    'depends_on_past': False,
    # 'start_date': datetime(2023, 1, 1),
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'catchup': False,

    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# 2. KHỞI TẠO DAG
with DAG(
    dag_id='us_oil_supply_chain_daily',
    default_args=default_args,
    description='ETL Pipeline: Load Imports + Production + Prices -> MySQL -> dbt',
    schedule_interval=None, 
    tags=['etl', 'oil', 'test'],
    max_active_runs=1,
) as dag:

    # TASK 1: Nạp Import
    load_imports = BashOperator(
        task_id='load_imports_csv',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_imports.py'
    )

    # TASK 2: Nạp Production
    load_production = BashOperator(
        task_id='load_production_csv',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_production.py'
    )

    # TASK 3 (MỚI): Nạp Giá Dầu từ API
    load_prices = BashOperator(
        task_id='load_oil_prices_api',
        # Lưu ý: Cần export DB_HOST để script biết kết nối vào đâu
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_prices.py'
    )

    # TASK 4: Chạy dbt Transformation
    # Lưu ý đường dẫn: /opt/airflow/oil_transformation (theo cấu hình bạn đã sửa ở Tuần 4)
    dbt_transform = BashOperator(
        task_id='dbt_transform_models',
        bash_command='cd /opt/airflow/oil_transformation && /home/airflow/dbt_venv/bin/dbt deps && /home/airflow/dbt_venv/bin/dbt build --profiles-dir .',
        env={
            'DB_HOST': 'mysql',        # <--- QUAN TRỌNG: Chỉ định host là 'mysql'
            'DB_USER': 'root',         # Thay bằng user của bạn
            'DB_PASSWORD': 'root',     # Thay bằng pass của bạn
            'DB_PORT': '3306',
            'DB_DATABASE': 'oil_dw'
        },
        dag=dag
    )

    # 3. ĐỊNH NGHĨA THỨ TỰ CHẠY
    # Ba task load chạy song song. Khi CẢ BA xong hết -> Mới chạy dbt
    [load_imports, load_production, load_prices] >> dbt_transform