from dotenv import load_dotenv
from pyspark.sql import functions as f
import os
load_dotenv()

def load_data(spark, silver_df):
    gold_path = os.getenv("gold")
    
    # Add time columns
    gold_dff = silver_df.withColumn('pickup_hour',f.hour(f.col('pickup_datetime')))\
                         .withColumn('pickup_day',f.dayofmonth(f.col('pickup_datetime')))\
                         .withColumn('pickup_month',f.month(f.col('pickup_datetime')))
    
    print("Extracted Hour, Day, and Month from 'pickup_datetime'")
    
    # Convert abbreviations
    gold_dff = gold_dff.withColumn('store_and_fwd_flag',
                                     f.when(f.col('store_and_fwd_flag')=='N','NO')\
                                      .when(f.col('store_and_fwd_flag')=='Y','YES')\
                                      .otherwise('Unknown')
                                      )
    
    print("Converted 'store_and_fwd_flag' abbreviations to full form")

    gold_dff = gold_dff.withColumn('RatecodeID',f.col('RatecodeID').cast('int'))\
                         .withColumn('passenger_count', f.col('passenger_count').cast('int'))
    
    # Calculate trip duration
    gold_df = gold_dff.withColumn('trip_duration',
                                   f.floor((f.unix_timestamp(f.col('dropoff_datetime'))-
                                            f.unix_timestamp(f.col('pickup_datetime')))/60).cast('int'))
    
    print("Calculated trip duration in minutes")
    
    print(f'Total Trip records: {gold_df.count()}')
    
    gold_df.coalesce(1).\
        write.\
        mode("overwrite").\
        option("header", True).\
        parquet(gold_path)
        
    print("Nyc data Write to Gold Layer")

    return gold_df
  