import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_engine():
    host = os.getenv("POSTGRES_HOST", "postgres_dw")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "fin_etl_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Đọc dữ liệu từ file vào bảng Raw
def extract():
    file_path = "/opt/airflow/data/Adult_census.xlsx"
    logger.info("Reading data from %s", file_path)

    df = pd.read_excel(file_path, engine="openpyxl")
    logger.info("Read %d rows and %d columns", len(df), len(df.columns))

    engine = get_engine()
    df.to_sql("raw_data", engine, if_exists="replace", index=False)
    logger.info("Successfully loaded %d rows into raw_data table", len(df))
