from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'minh_nhat',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# @weekly: Backup chạy định kỳ vào mỗi cuối tuần (hoặc bạn có thể đổi thành @daily)
with DAG(
    '03_data_lake_backup', 
    default_args=default_args, 
    schedule_interval='@weekly', 
    catchup=False, 
    tags=['backup', 'datalake']
) as dag:

    # Khối lệnh Bash thực hiện 3 việc:
    # 1. Tạo thư mục backups (nếu chưa có)
    # 2. Xóa các file backup cũ hơn 7 ngày (để tiết kiệm ổ cứng)
    # 3. Nén toàn bộ thư mục data_lake thành file .tar.gz có gắn ngày tháng
    backup_command = """
    mkdir -p /opt/airflow/backups && \
    find /opt/airflow/backups -type f -name "*.tar.gz" -mtime +7 -delete && \
    tar -czvf /opt/airflow/backups/datalake_backup_$(date +%Y%m%d_%H%M).tar.gz -C /opt/airflow data_lake
    """

    # Task Backup
    backup_task = BashOperator(
        task_id='compress_and_backup_datalake',
        bash_command=backup_command
    )