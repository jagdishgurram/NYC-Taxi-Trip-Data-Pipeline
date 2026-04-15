from utils.spark_config import get_spark_session
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.star import create_star_schema
from validation.validate import run_all_validations
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
        logger.info("Bronze layer created")
        
        silver_df = transform_data(raw_df)
        logger.info("Silver layer created")
        
        run_all_validations(silver_df)
        
        gold_df = load_data(spark, silver_df)
        logger.info("Gold layer ready")
        
        final_df = create_star_schema(spark, gold_df)
        
        logger.info("Pipeline completed")
        
        # final_analytic(spark, final_df) # For Final Analysis Perform
    
    except Exception as e:
        logger.error(f"Pipeline Failed: {e}")
    
    finally:
        spark.stop()
        logger.info("Spark Session Stopped")
    
if __name__ == "__main__":
    run_pipeline()
