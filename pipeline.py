from configs.spark_config import get_spark_session
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.star import create_star_schema
from validation.validate import run_all_validations


def run_pipeline():
    
    spark = get_spark_session()
    print("SparkSession Created")
    
    raw_df = extract_data(spark)
    print("===========================================", flush=True)
    print("Data Extracted & Stored To Bronze Layer", flush=True)
    print("===========================================", flush=True)
    
    silver_df = transform_data(raw_df)
    print("===========================================", flush=True)
    print("Transformed & Stored To Silver Layer", flush=True)
    print("===========================================", flush=True)
    
    run_all_validations(silver_df)
    
    gold_df = load_data(spark, silver_df)
    print("===========================================", flush=True)
    print("Finalised Data For Star Schema Process", flush=True)
    print("===========================================", flush=True)
    
    create_star_schema(spark, gold_df)
    
    print("Pipeline executed successfully", flush=True)
    
    spark.stop()
    
    print("Spark Session Stopped", flush=True)
    
if __name__ == "__main__":
    run_pipeline()