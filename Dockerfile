
# Sử dụng image gốc của Airflow
FROM apache/airflow:2.10.2

# Chuyển sang quyền root để cài thêm gói hệ thống (nếu cần)
USER root
RUN apt-get update && apt-get install -y git

# Chuyển về user airflow để cài gói Python
USER airflow
RUN pip install --no-cache-dir \
    pandas \
    sqlalchemy \
    pymysql
# Cập nhật PATH để hệ thống nhận diện các tool cài qua pip
RUN python -m venv /home/airflow/dbt_venv
RUN /home/airflow/dbt_venv/bin/pip install --upgrade pip
# Cài đặt dbt và các thư viện cần thiết
RUN /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
    "protobuf>=4.21.0,<5.0.0" \
    "dbt-core==1.7.9" \
    "dbt-mysql==1.7.0" \
    pandas \
    sqlalchemy \
    pymysql
