from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'minh_nhat',
    'start_date': datetime(2023, 1, 1),
}

# @once: DAG này chỉ chạy 1 lần duy nhất
with DAG('01_init_oil_history', default_args=default_args, schedule_interval='@once', catchup=False, tags=['init', 'parquet', 'duckdb']) as dag:

    # Task 1: Nạp file CSV Imports -> lưu thành Parquet
    load_imports = BashOperator(
        task_id='load_imports_parquet',
        bash_command='python /opt/airflow/scripts/etl_imports.py'
    )

    # Task 2: Nạp file CSV Production -> lưu thành Parquet
    load_production = BashOperator(
        task_id='load_production_parquet',
        bash_command='python /opt/airflow/scripts/etl_production.py'
    )

    # Task 3: Lấy giá dầu lịch sử (Monthly) -> lưu thành Parquet
    load_prices = BashOperator(
        task_id='load_oil_prices_initial',
        bash_command='python /opt/airflow/scripts/etl_prices.py'
    )

    # Task 4: Chạy DBT (Đã gỡ bỏ toàn bộ env kết nối MySQL rườm rà)
    dbt_transform = BashOperator(
        task_id='dbt_full_build',
        bash_command='cd /opt/airflow/oil_transformation && /home/airflow/dbt_venv/bin/dbt deps && /home/airflow/dbt_venv/bin/dbt build --profiles-dir .'
    )

    # Luồng chạy: 3 task nạp chạy song song -> Sau đó chạy DBT
    [load_imports, load_production, load_prices] >> dbt_transform