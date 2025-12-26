# US Oil Supply Chain Analytics 🛢️

Dự án Data Engineering xây dựng kho dữ liệu (Data Warehouse) để phân tích mức độ phụ thuộc năng lượng của Mỹ vào nhập khẩu so với sản xuất nội địa.

## 📊 Dashboard Result
![Dashboard Preview](https://github.com/lmnhat287/US_Oil_Supply_Analytics/blob/main/Screenshot%202025-12-27%20053523.png)
![Dashboard Preview](https://github.com/lmnhat287/US_Oil_Supply_Analytics/blob/main/Screenshot%202025-12-27%20053733.png)
![Dashboard Preview](https://github.com/lmnhat287/US_Oil_Supply_Analytics/blob/main/Screenshot%202025-12-27%20053743.png)
*(Ảnh chụp kết quả phân tích trên Power BI)*

## 🛠️ Tech Stack
- **Infrastructure:** Docker, Docker Compose
- **Database:** MySQL 8.0
- **ETL:** Python (Pandas, SQLAlchemy)
- **Transformation:** dbt Core (Data Build Tool)
- **Visualization:** Power Bi
- **Automatic:** Airflow

## 🚀 How to run

Các hướng dẫn dưới đây cập nhật cho phiên bản Airflow đã được nâng cấp. Hướng dẫn minh họa sử dụng Docker Compose (Docker Compose V2 - `docker compose`). Nếu bạn đang dùng `docker-compose` (V1) hãy thay đổi lệnh tương ứng.

1. Chuẩn bị
   - Cài đặt Docker và Docker Compose. Kiểm tra bằng `docker --version` và `docker compose version`.
   - Sao chép file môi trường mẫu và chỉnh sửa các biến cần thiết:

     ```bash
     cp .env.example .env
     # chỉnh giá trị như MYSQL_ROOT_PASSWORD, MYSQL_DATABASE, AIRFLOW__CORE__FERNET_KEY, v.v.
     ```

2. Khởi động cơ sở dữ liệu

   - Đảm bảo MySQL được cấu hình trong `docker-compose.yml`. Khởi động MySQL trước (nếu bạn muốn):

     ```bash
     docker compose up -d mysql
     # hoặc khởi động toàn bộ stack:
     docker compose up -d --build
     ```

3. Cài đặt và chạy dbt (tùy chọn, để build models vào data warehouse)

   - Vào container hoặc trên máy host có môi trường dbt cấu hình:

     ```bash
     # Cài đặt dependencies và thực thi dbt
     dbt deps
     dbt seed
     dbt run
     ```

4. Khởi tạo Airflow (bước quan trọng sau khi cập nhật Airflow)

   - Nếu `docker-compose.yml` có service để khởi tạo Airflow (ví dụ `airflow-init`), chạy nó:

     ```bash
     docker compose run --rm airflow airflow db init
     docker compose run --rm airflow airflow users create \
       --username admin --firstname Admin --lastname User --role Admin --email admin@example.com
     ```

   - Một số cấu hình Compose dùng entrypoint `airflow scheduler` / `airflow webserver`. Các lệnh trên đảm bảo metadata database được khởi tạo và tạo user admin.

5. Khởi động Airflow webserver và scheduler

   ```bash
   docker compose up -d
   # hoặc chỉ start airflow service nếu muốn
   docker compose up -d webserver scheduler
   ```

6. Kiểm tra và trigger DAGs

   - Mở UI Airflow: http://localhost:8080
   - Đăng nhập bằng user đã tạo (username: `admin` trong ví dụ). Kích hoạt hoặc trigger các DAG cần chạy.

7. Logs & troubleshooting

   - Xem logs service:

     ```bash
     docker compose logs -f webserver
     docker compose logs -f scheduler
     ```

   - Nếu gặp lỗi migration/permission sau khi nâng Airflow, thử xóa và khởi tạo lại metadata DB (chú ý mất dữ liệu DAG run cũ):

     ```bash
     docker compose run --rm airflow airflow db reset
     docker compose run --rm airflow airflow db init
     ```

Ghi chú:
- Tên services (ví dụ `mysql`, `webserver`, `scheduler`, `airflow`) có thể khác trong `docker-compose.yml` của repo — điều chỉnh lệnh cho phù hợp.
- Nếu sử dụng executor phân tán (Celery, Kubernetes), cần cấu hình thêm broker (Redis/RabbitMQ) và workers.


---

(Các phần khác trong README giữ nguyên.)
