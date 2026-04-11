from dotenv import load_dotenv
from pyspark.sql import functions as f
import os
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv()

def create_star_schema(spark,gold_df):
    dataset_path = os.getenv("dataset_path")
    
    # Register temp view for SQL queries
    spark.sql("CREATE DATABASE IF NOT EXISTS nyc_taxitrip_db")
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.nyc_trip_table")
    spark.sql(f"""
    CREATE TABLE nyc_taxitrip_db.nyc_trip_table
    USING PARQUET
    LOCATION '{dataset_path}/gold'
    """)
    logger.info("Database & NycTable Created")
    
    ## CREATING DIM PAYMENT 
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.dim_payment")
    spark.sql(f"""
              SELECT
              payment_type AS payment_type_id,
              CASE
              WHEN payment_type = 1 THEN 'Cash'
              WHEN payment_type = 2 THEN 'Credit Card'
              WHEN payment_type = 3 THEN 'Dispute'
              WHEN payment_type = 4 THEN 'No Charges'
              ELSE 'Other'
              END AS payment_type
              FROM (
                  SELECT distinct(payment_type) FROM nyc_taxitrip_db.nyc_trip_table
              ) temp
              ORDER BY payment_type_id Asc
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/dim_payment')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.dim_payment
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/dim_payment'
              """)

    ## CREATING DIM DATE 
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.dim_date")
    gold_df.select(
        f.to_date('pickup_datetime').alias('pickup_date')
        ).distinct()\
            .withColumn("date_id",f.date_format("pickup_date","yyyyMMdd").cast("int"))\
            .withColumn("year",f.year('pickup_date'))\
            .withColumn("month",f.month('pickup_date'))\
            .withColumn("day",f.dayofmonth('pickup_date'))\
            .withColumn("week",f.dayofweek('pickup_date'))\
            .createOrReplaceTempView('TempDate')
            
    spark.sql(f"""
              SELECT
              date_id,
              year,
              month,
              day,
              week
              FROM TempDate
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/dim_date')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.dim_date
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/dim_date'
              """)
    
    ## CREATING DIM LOCATION
    spark.read.csv(f'{dataset_path}/taxi_zone_lookup.csv', 
                   header=True, inferSchema=True)\
                       .createOrReplaceTempView('TempLocationZone')
                       
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.dim_location")
    
    spark.sql("""
              SELECT pickup_location_id FROM nyc_taxitrip_db.nyc_trip_table
              UNION
              SELECT dropoff_location_id FROM nyc_taxitrip_db.nyc_trip_table
              """).createOrReplaceTempView('locationTemp')

    
    spark.sql(f"""
              SELECT DISTINCT
              t.LocationID as location_id,
              t.Borough as borough,
              t.Zone as zone,
              t.service_zone
              FROM TempLocationZone t
              JOIN locationTemp l
              ON t.LocationID = l.pickup_location_id
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/dim_location')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.dim_location
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/dim_location'
              """)
    
    ## CREATING DIM VENDOR
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.dim_vendor")
    spark.sql(f"""
              SELECT
              VendorID AS vendor_id,
              CASE
              WHEN VendorID = 1 THEN 'Creative Mobile Technologies, LLC'
              WHEN VendorID = 2 THEN 'VeriFone Inc'
              WHEN VendorID = 4 THEN 'MTData'
              ELSE 'Unknown'
              END AS vendor_name
              FROM (
                  SELECT DISTINCT(VendorID) FROM nyc_taxitrip_db.nyc_trip_table
              ) temp
              ORDER BY VendorID ASC
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/dim_vendor')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.dim_vendor
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/dim_vendor'
              """)
    
    ## CREATING DIM RATECODE
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.dim_ratecode")
    spark.sql(f"""
              SELECT DISTINCT
              RatecodeID AS ratecode_id,
              CASE
              WHEN RatecodeID = 1 THEN 'Standard rate'
              WHEN RatecodeID = 2 THEN 'JFK Airport'
              WHEN RatecodeID = 3 THEN 'Newark Airport'
              WHEN RatecodeID = 4 THEN 'Nassau / Westchester'
              WHEN RatecodeID = 5 THEN 'Negotiated Fare'
              WHEN RatecodeID = 6 THEN 'Group Ride'
              WHEN RatecodeID = 99 THEN 'Unknown'
              ELSE 'Unknown'
              END AS ratecode_name
              FROM (
                  SELECT DISTINCT(RatecodeID) FROM nyc_taxitrip_db.nyc_trip_table
              ) temp
              ORDER BY ratecode_id ASC
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/dim_ratecode')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.dim_ratecode
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/dim_ratecode'
              """)
    
    ## CREATING FACT TRIP
    spark.sql(f"DROP TABLE IF EXISTS nyc_taxitrip_db.facts_trip")   
    spark.sql(f"""
              SELECT
              monotonically_increasing_id() AS trip_id, -- SURROGATE KEY
              VendorID AS vendor_id, -- FOREIGN KEY
              CAST(date_format(to_date(pickup_datetime), 'yyyyMMdd') AS INT) AS date_id, -- FOREIGN KEY
              pickup_location_id, -- FOREIGN KEY
              dropoff_location_id, -- FOREIGN KEY
              payment_type AS payment_type_id, -- FOREIGN KEY
              RatecodeID AS ratecode_id, -- FOREIGN KEY
              passenger_count,
              trip_distance,
              fare_amount,
              total_amount,
              store_and_fwd_flag,
              trip_duration
              FROM nyc_taxitrip_db.nyc_trip_table
              """).write.mode("overwrite").format("csv").option("header", True).save(f'{dataset_path}/facts_dimension/facts_trip')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.facts_trip
              USING CSV
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/facts_trip'
              """)
    
    logger.info("Star Schema Created Successfully")
    
    ## Creating facts-dimension table
    spark.sql("DROP TABLE IF EXISTS nyc_taxitrip_db.trip_analytics")
    logger.info("Running final analysis")
    spark.sql(f"""
              SELECT
              f.trip_id,
              
              -- Date Dimension
              d.day,
              d.month,
              d.year,
              
              -- Location Dimensions
              pu.borough AS pickup_borough,
              pu.zone AS pickup_zone,
              do.borough AS dropoff_borough,
              do.zone AS dropoff_zone,
              
              -- Vendor
              v.vendor_name,
              
              -- Trip Metrics
              f.passenger_count,
              f.trip_distance,
              f.fare_amount,
              f.total_amount,
              f.trip_duration,
              
              -- Ratecode
              r.ratecode_name,
              
              -- Payment
              p.payment_type
              
              FROM nyc_taxitrip_db.facts_trip f
              LEFT JOIN nyc_taxitrip_db.dim_date d 
              ON f.date_id = d.date_id
              LEFT JOIN nyc_taxitrip_db.dim_location pu 
              ON f.pickup_location_id = pu.location_id
              LEFT JOIN nyc_taxitrip_db.dim_location do 
              ON f.dropoff_location_id = do.location_id
              LEFT JOIN nyc_taxitrip_db.dim_vendor v 
              ON f.vendor_id = v.vendor_id
              LEFT JOIN nyc_taxitrip_db.dim_ratecode r 
              ON f.ratecode_id = r.ratecode_id
              LEFT JOIN nyc_taxitrip_db.dim_payment p 
              ON f.payment_type_id = p.payment_type_id
              """).write.mode("overwrite").format("PARQUET").save(f'{dataset_path}/facts_dimension/trip_analytics')
    
    spark.sql(f"""
              CREATE TABLE nyc_taxitrip_db.trip_analytics
              USING PARQUET
              OPTIONS (header "true")
              LOCATION '{dataset_path}/facts_dimension/trip_analytics'
              """)
    
    trip_analytics = spark.sql("SELECT * FROM nyc_taxitrip_db.trip_analytics")
    
    return trip_analytics
