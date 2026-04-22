from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'minh_nhat',
    'start_date': datetime(2024, 1, 1), # Khớp với start_date của dự án
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# @daily: Chạy hàng ngày vào 00:00
with DAG('02_daily_price_update', default_args=default_args, schedule_interval='@daily', catchup=False, tags=['daily', 'dlt', 'duckdb']) as dag:

    # Task 1: Chạy DLT Pipeline lấy giá dầu Daily nạp thẳng vào DuckDB
    load_prices_dlt = BashOperator(
        task_id='load_oil_prices_dlt',
        bash_command='python /opt/airflow/scripts/dlt_eia_pipeline.py'
    )

    # Task 2: Cập nhật các chỉ số EIA Advanced -> lưu thành Parquet
    load_eia_advanced = BashOperator(
        task_id='load_eia_advanced',
        bash_command='python /opt/airflow/scripts/etl_eia_advanced.py'
    )

    # Task 3: Chạy DBT gộp dữ liệu
    dbt_transform = BashOperator(
        task_id='dbt_update_models',
        bash_command='cd /opt/airflow/oil_transformation && /home/airflow/dbt_venv/bin/dbt build --profiles-dir .'
    )

    # Luồng chạy: 2 task lấy dữ liệu API chạy song song -> Sau đó gọi dbt
    [load_prices_dlt, load_eia_advanced] >> dbt_transform