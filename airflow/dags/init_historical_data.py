from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'minh_nhat',
    'start_date': datetime(2023, 1, 1),
}

# @once: DAG này chỉ chạy 1 lần duy nhất rồi dừng mãi mãi
with DAG('01_init_oil_history', default_args=default_args, schedule_interval='@once', catchup=False, tags=['init']) as dag:

    # Task 1: Nạp CSV Imports (Nặng)
    load_imports = BashOperator(
        task_id='load_imports_csv',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_imports.py',
        env={'DB_HOST': 'mysql'} # Config như cũ
    )

    # Task 2: Nạp CSV Production (Nặng)
    load_production = BashOperator(
        task_id='load_production_csv',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_production.py',
        env={'DB_HOST': 'mysql'}
    )

    # Task 3: Nạp Price lần đầu (để đủ dữ liệu)
    load_prices = BashOperator(
        task_id='load_oil_prices_initial',
        bash_command='export DB_HOST=mysql && python /opt/airflow/scripts/etl_prices.py',
        env={'DB_HOST': 'mysql'}
    )

    # Task 4: Chạy DBT (Full Build lần đầu)
    dbt_transform = BashOperator(
        task_id='dbt_full_build',
        # Nhớ dùng đường dẫn dbt_venv
        bash_command='cd /opt/airflow/oil_transformation && /home/airflow/dbt_venv/bin/dbt deps && /home/airflow/dbt_venv/bin/dbt build --profiles-dir .',
        env={
            'DB_HOST': 'mysql', 'DB_PORT': '3306', 'DB_USER': 'root', 'DB_PASSWORD': 'root', 'DB_DATABASE': 'oil_dw'
        }
    )

    # Luồng chạy: 3 task nạp chạy song song -> Sau đó chạy DBT
    [load_imports, load_production, load_prices] >> dbt_transform