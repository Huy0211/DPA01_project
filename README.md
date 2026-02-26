# ETL Pipeline với Airflow và PostgreSQL

## Cấu trúc thư mục
```
/
├── dags/
│   └── fin_etl_project.py          # DAG chính
├── data/
│   └── Adult_census.xlsx           # File dữ liệu đầu vào
├── logs/                           # Log của Airflow
├── plugins/
│   ├── extract.py                  # Module extract
│   ├── transform.py                # Module transform
│   └── load.py                     # Module load
├── docker-compose.yaml            # Docker compose config
├── dockerfile                     # Dockerfile
├── .dockerignore                  # Docker ignore
├── .gitignore                     # Git ignore
└── requirements.txt               # Python dependencies
```

## Cách chạy

### 1. Khởi động tất cả services
```bash
docker-compose up -d
```

### 2. Truy cập Airflow UI
- URL: http://localhost:8081
- Username: admin
- Password: admin

### 3. Chạy DAG
- Vào Airflow UI
- Tìm DAG `fin_etl_project`
- Bật DAG và trigger run

## Mô tả pipeline

### Extract (extract.py)
- Đọc dữ liệu từ file Excel `Adult_census.xlsx`
- Load dữ liệu vào bảng `raw_data` trong PostgreSQL

### Transform (transform.py)
- Lấy dữ liệu từ bảng `raw_data`
- Làm sạch dữ liệu
- Kiểm tra validation
- Encoding các cột categorical bằng LabelEncoder
- Lưu vào bảng `transformed_data`

### Load (load.py)
- Lấy dữ liệu từ bảng `transformed_data`
- Load dữ liệu vào bảng `data_warehouse`

## Dừng services
```bash
docker-compose down
```

## Xóa toàn bộ (bao gồm volumes)
```bash
docker-compose down -v
```
