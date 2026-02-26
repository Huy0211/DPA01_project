import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Valid values ---------
VALID_WORKCLASS = {
    "Private", "Self-emp-not-inc", "Self-emp-inc", "Federal-gov",
    "Local-gov", "State-gov", "Without-pay", "Never-worked",
}

VALID_EDUCATION = {
    "Bachelors", "HS-grad", "11th", "Masters", "9th", "Some-college",
    "Assoc-acdm", "Assoc-voc", "7th-8th", "Doctorate", "Prof-school",
    "5th-6th", "10th", "1st-4th", "Preschool", "12th",
}

VALID_MARITAL_STATUS = {
    "Never-married", "Married-civ-spouse", "Divorced",
    "Married-spouse-absent", "Separated", "Married-AF-spouse", "Widowed",
}

VALID_OCCUPATION = {
    "Tech-support", "Craft-repair", "Other-service", "Sales",
    "Exec-managerial", "Prof-specialty", "Handlers-cleaners",
    "Machine-op-inspct", "Adm-clerical", "Farming-fishing",
    "Transport-moving", "Priv-house-serv", "Protective-serv", "Armed-Forces",
}

VALID_RELATIONSHIP = {
    "Wife", "Own-child", "Husband", "Not-in-family",
    "Other-relative", "Unmarried",
}

VALID_RACE = {
    "White", "Black", "Asian-Pac-Islander", "Amer-Indian-Eskimo", "Other",
}

VALID_SEX = {"Male", "Female"}


def get_engine():
    """Create a SQLAlchemy engine from environment variables."""
    host = os.getenv("POSTGRES_HOST", "postgres_dw")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_DB", "fin_etl_db")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


# -----------------------------------------------------------------
# Clean data
# -----------------------------------------------------------------

def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Chuẩn hóa tên cột
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace("-", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # Bỏ khoảng trắng ở các cột
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # Drop các dòng có giá trị rỗng
    df.replace(["?", "nan", "NaN", ""], pd.NA, inplace=True)
    df.dropna(inplace=True)

    logger.info("After cleaning: %d rows remain", len(df))
    return df


# -----------------------------------------------------------------
# Validation
# -----------------------------------------------------------------

# Kiểm tra các giá trị trong cột có hợp lệ không
def validate_column_values(series: pd.Series, valid_set: set, col_name: str):
    invalid = set(series.unique()) - valid_set
    if invalid:
        raise ValueError(
            f"Validation failed for '{col_name}': invalid values {invalid}"
        )

# Kiểm tra toàn bộ dataframe
def validate(df: pd.DataFrame):
    errors = []

    # --- age: positive integer, 0-120 ---
    if not pd.api.types.is_numeric_dtype(df["age"]):
        errors.append("age must be numeric")
    elif (df["age"] < 0).any() or (df["age"] > 120).any():
        errors.append("age must be between 0 and 120")

    # --- workclass ---
    try:
        validate_column_values(df["workclass"], VALID_WORKCLASS, "workclass")
    except ValueError as e:
        errors.append(str(e))

    # --- fnlwgt: positive numeric, not null ---
    if not pd.api.types.is_numeric_dtype(df["fnlwgt"]):
        errors.append("fnlwgt must be numeric")
    elif df["fnlwgt"].isnull().any():
        errors.append("fnlwgt must not contain null values")
    elif (df["fnlwgt"] <= 0).any():
        errors.append("fnlwgt must be positive")

    # --- education ---
    try:
        validate_column_values(df["education"], VALID_EDUCATION, "education")
    except ValueError as e:
        errors.append(str(e))

    # --- education_num: integer 1-16 ---
    if not pd.api.types.is_numeric_dtype(df["education_num"]):
        errors.append("education_num must be numeric")
    elif (df["education_num"] < 1).any() or (df["education_num"] > 16).any():
        errors.append("education_num must be between 1 and 16")

    # --- marital_status ---
    try:
        validate_column_values(df["marital_status"], VALID_MARITAL_STATUS, "marital_status")
    except ValueError as e:
        errors.append(str(e))

    # --- occupation ---
    try:
        validate_column_values(df["occupation"], VALID_OCCUPATION, "occupation")
    except ValueError as e:
        errors.append(str(e))

    # --- relationship ---
    try:
        validate_column_values(df["relationship"], VALID_RELATIONSHIP, "relationship")
    except ValueError as e:
        errors.append(str(e))

    # --- race ---
    try:
        validate_column_values(df["race"], VALID_RACE, "race")
    except ValueError as e:
        errors.append(str(e))

    # --- sex ---
    try:
        
        validate_column_values(df["sex"], VALID_SEX, "sex")
    except ValueError as e:
        errors.append(str(e))

    # --- capital_gain: numeric >= 0 ---
    if not pd.api.types.is_numeric_dtype(df["capital_gain"]):
        errors.append("capital_gain must be numeric")
    elif (df["capital_gain"] < 0).any():
        errors.append("capital_gain must be >= 0")

    # --- capital_loss: numeric >= 0 ---
    if not pd.api.types.is_numeric_dtype(df["capital_loss"]):
        errors.append("capital_loss must be numeric")
    elif (df["capital_loss"] < 0).any():
        errors.append("capital_loss must be >= 0")

    # --- hours_per_week: integer 1-100 ---
    if not pd.api.types.is_numeric_dtype(df["hours_per_week"]):
        errors.append("hours_per_week must be numeric")
    elif (df["hours_per_week"] < 1).any() or (df["hours_per_week"] > 100).any():
        errors.append("hours_per_week must be between 1 and 100")

    # Báo tất cả lỗi nếu có
    if errors:
        raise ValueError("Data validation failed:\n  - " + "\n  - ".join(errors))

    logger.info("All validation checks passed")


# -----------------------------------------------------------------
# Hàm transform chính
# -----------------------------------------------------------------

def transform():
    engine = get_engine()

    logger.info("Reading data from raw_data table")
    df = pd.read_sql_table("raw_data", engine)
    logger.info("Read %d rows", len(df))

    # Step 1: Clean
    df = clean(df)

    # Step 2: Validate
    validate(df)

    # Step 3: Encode các cột categorical
    label_encoders = {}
    cat_cols = df.select_dtypes(include="object").columns
    for col in cat_cols:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
        label_encoders[col] = le

    logger.info("Encoded %d categorical columns: %s", len(cat_cols), list(cat_cols))

    # Step 4: Lưu vào bảng transformed_data
    df.to_sql("transformed_data", engine, if_exists="replace", index=False)
    logger.info("Successfully loaded %d rows into transformed_data table", len(df))
