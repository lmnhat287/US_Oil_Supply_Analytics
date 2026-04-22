terraform {
  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0.1"
    }
  }
}

provider "docker" {}

# 1. Tạo Mạng nội bộ cho hệ thống
resource "docker_network" "data_network" {
  name = "zoomcamp_network"
}

# 2. Tạo Volume để bảo vệ dữ liệu Postgres
resource "docker_volume" "postgres_data" {
  name = "postgres_metadata_volume"
}

# 3. Triển khai Postgres (Làm Database cho Airflow)
resource "docker_container" "postgres" {
  image = "postgres:13"
  name  = "postgres_backend"
  networks_advanced {
    name = docker_network.data_network.name
  }
  env = [
    "POSTGRES_USER=airflow",
    "POSTGRES_PASSWORD=airflow",
    "POSTGRES_DB=airflow"
  ]
  volumes {
    container_path = "/var/lib/postgresql/data"
    volume_name    = docker_volume.postgres_data.name
  }
  ports {
    internal = 5432
    external = 5432
  }
}

# 4. Triển khai Apache Airflow
resource "docker_container" "airflow" {
  image = "apache/airflow:2.7.3"
  name  = "airflow_orchestrator"
  
  # Khởi động webserver (Scheduler sẽ chạy nền hoặc thông qua LocalExecutor)
  command = ["webserver"]
  
  networks_advanced {
    name = docker_network.data_network.name
  }
  
  # Cấu hình biến môi trường kết nối Airflow với Postgres
  env = [
    "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres_backend/airflow",
    "AIRFLOW__CORE__EXECUTOR=LocalExecutor",
    "AIRFLOW__CORE__LOAD_EXAMPLES=False",
    "_AIRFLOW_DB_UPGRADE=True",
    "_AIRFLOW_WWW_USER_CREATE=True",
    "_AIRFLOW_WWW_USER_USERNAME=admin",
    "_AIRFLOW_WWW_USER_PASSWORD=admin",
    "_PIP_ADDITIONAL_REQUIREMENTS=dlt duckdb==0.10.0",
    "_AIRFLOW_USER_CMD=apt-get update && apt-get install -y gcc g++ python3-dev"
  ]
  
  ports {
    internal = 8080
    external = 8080
  }
  
  # Ánh xạ thư mục máy thật vào container
  # (Sử dụng đường dẫn tương đối của Terraform)
  volumes {
    host_path      = "${abspath(path.cwd)}/../airflow/dags"
    container_path = "/opt/airflow/dags"
  }
  volumes {
    host_path      = "${abspath(path.cwd)}/../scripts"
    container_path = "/opt/airflow/scripts"
  }
  volumes {
    host_path      = "${abspath(path.cwd)}/../data_lake"
    container_path = "/opt/airflow/data_lake"
  }
}