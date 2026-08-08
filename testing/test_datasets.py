import pytest
from pyspark.sql import functions as f
from etl.extract import extract_data
from etl.transform import transform_data
from utils.spark_config import get_spark_session

@pytest.fixture
def  transform_func():
    spark = get_spark_session()
    df = extract_data(spark)
    transform_df = transform_data(df)
    return transform_df
    
def test_data_not_empty(transform_func):
    assert transform_func.limit(1).count() > 0, (
        "Extracted data is empty!!"
    )
    
def test_validate_nulls(transform_func):
    columns = ["VendorID", "pickup_datetime"]
    for col in columns:
        null_count = transform_func.filter(f.col(col).isNull()).count()
        
        assert null_count == 0, (
            f"Column {col} has {null_count} Null Values"
        )

def test_validate_negative_values(transform_func):
    
    for col in ["trip_distance", "fare_amount"]:
        negative_count = transform_func.filter(f.col(col)<0).count()
    
        assert negative_count == 0,(
            f"Column {col} has {negative_count} Negative Values"
        )
            
def test_passenger_count(transform_func):
    invalid = transform_func.filter(f.col("passenger_count") <= 0).count()
    assert invalid == 0, (
        f"{invalid} invalid passenger counts detected"
    )
        