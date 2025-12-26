from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'minh_nhat',
    'start_date': datetime(2025, 12, 20), # Đặt ngày bắt đầu là hôm qua/hôm nay
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# @daily: Chạy hàng ngày vào 00:00
with DAG('02_daily_price_update', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['daily']) as dag:

    # Task 1: Chỉ chạy mỗi API Giá dầu (Nhẹ)
    load_prices = BashOperator(
        task_id='load_oil_prices_api',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_prices.py',
        env={'DB_HOST': 'mysql'}
    )

    #Task 2: Chạy ETL nâng cao từ EIA (Lấy thêm các chỉ số khác)
    load_eia_advanced = BashOperator(
        task_id='load_eia_advanced',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_eia_advanced.py',
        env={'DB_HOST': 'mysql'}
    )

    # Task 3: Chạy DBT để cập nhật View/Table báo cáo
    # Mẹo: DBT sẽ lấy dữ liệu imports/production ĐÃ CÓ (từ DAG 1) + dữ liệu giá MỚI (từ Task 1) để gộp lại.
    dbt_transform = BashOperator(
        task_id='dbt_update_models',
        bash_command='cd /opt/airflow/oil_transformation && /home/airflow/dbt_venv/bin/dbt build --profiles-dir .',
        env={
            'DB_HOST': 'mysql', 'DB_PORT': '3306', 'DB_USER': 'root', 'DB_PASSWORD': 'root', 'DB_DATABASE': 'oil_dw'
        }
    )

    [load_prices, load_eia_advanced] >> dbt_transform