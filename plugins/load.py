import os
import logging
import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    host = os.getenv("POSTGRES_HOST", "postgres_dw")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "fin_etl_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")

# Load dữ liệu vào bảng data_warehouse phục vụ ML
def load():
    engine = get_engine()

    logger.info("Reading data from transformed_data table")
    df = pd.read_sql_table("transformed_data", engine)
    logger.info("Read %d rows", len(df))

    df.to_sql("data_warehouse", engine, if_exists="replace", index=False)
    logger.info("Successfully loaded %d rows into data_warehouse table", len(df))
