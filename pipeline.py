from utils.spark_config import get_spark_session
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.star import create_star_schema
from testing import test_datasets
from analytics.final_trip_analysis import final_analytic
from utils.logger import get_logger

logger = get_logger(__name__)

def run_pipeline():
    
    try:
        spark = get_spark_session()
        logger.info("SparkSession Created")
    except Exception as e:
        logger.error(f"SparkSession Failed: {e}")
        return
    
    try:
        raw_df = extract_data(spark)
        
        silver_df = transform_data(raw_df)
        
        test_datasets.test_data_not_empty(silver_df)
        test_datasets.test_passenger_count(silver_df)
        test_datasets.test_validate_negative_values(silver_df)
        test_datasets.test_validate_nulls(silver_df)
        
        gold_df = load_data(spark, silver_df)
        
        final_df = create_star_schema(spark, gold_df)
        
        logger.info("Pipeline completed")
        
        final_analytic(spark, final_df) # For Final Analysis Perform
    
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
    
    finally:
        spark.stop()
        logger.info("Spark Session Stopped")
    
if __name__ == "__main__":
    run_pipeline()