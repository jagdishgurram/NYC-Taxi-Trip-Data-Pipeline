from dotenv import load_dotenv
from pyspark.sql import functions as f
import os
from utils.logger import get_logger

load_dotenv()

logger = get_logger(__name__)

def transform_data(df):
    dataset_path = os.getenv("dataset_path")

    bronze_df = df.select('VendorID',
                          'tpep_pickup_datetime',
                          'tpep_dropoff_datetime',
                          'PULocationID',
                          'DOLocationID',
                          'passenger_count',
                          'trip_distance',
                          'fare_amount',
                          'total_amount',
                          'payment_type',
                          'RatecodeID',
                          'store_and_fwd_flag'
                          ).withColumnRenamed('tpep_pickup_datetime','pickup_datetime')\
                            .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')\
                            .withColumnRenamed('PULocationID','pickup_location_id')\
                            .withColumnRenamed('DOLocationID','dropoff_location_id')
                            
    logger.info("Filtered and renamed columns") 
    
    before_filter = bronze_df.count()
    
    # Filter invalid rows 
    silver_df = bronze_df.filter(
    (f.col('trip_distance')>0)&
    (f.col('fare_amount')>0)&
    (f.col('total_amount')>0)&
    (f.col("passenger_count")>0)
    ).dropna()
    
    after_filter = silver_df.count()
    logger.info(f"Records before cleaning: {before_filter}")
    logger.info(f"Records after cleaning & dropna: {after_filter}")

    silver_df.coalesce(1).\
        write.\
        mode("overwrite").\
        option("header", True).\
        parquet(f"{dataset_path}/silver")

    return silver_df