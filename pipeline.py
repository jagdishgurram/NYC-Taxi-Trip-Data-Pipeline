from configs.spark_config import get_spark_session
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.star import create_star_schema


def run_pipeline():
    
    spark = get_spark_session()
    
    raw_df = extract_data(spark)
    
    silver_df = transform_data(raw_df)
    
    gold_df = load_data(spark, silver_df)
    
    starSchema = create_star_schema(spark, gold_df)
    
    print("Pipeline executed successfully")
    
if __name__ == "__main__":
    run_pipeline()