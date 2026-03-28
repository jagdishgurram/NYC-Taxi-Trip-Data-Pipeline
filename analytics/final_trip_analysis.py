from configs.spark_config import get_spark_session

spark = get_spark_session()
final_df = spark.read.parquet("C:/Users/JAGDISH/OneDrive/Desktop/NYC-Taxi-Trip-Data-Pipeline/datasets/facts_dimension/final_trip_analysis")
final_df.createOrReplaceTempView("final_trip_analysis")

## ===========================================================================
## Total revenue by month
## ===========================================================================
print("Total revenue by month")
spark.sql("""
          SELECT month, FORMAT_NUMBER(sum(total_amount),2) as revenue FROM final_trip_analysis
          GROUP BY month
          ORDER BY month
          """).show()

## ===========================================================================
## Most popular pickup zone
## ===========================================================================
print("Most popular pickup zone")
spark.sql("""
 SELECT pickup_zone, count(pickup_zone) as count FROM final_trip_analysis
 GROUP BY pickup_zone
 ORDER BY count DESC
""").show(5,truncate=False)

## ===========================================================================
## Which payment method generates the most revenue?
## ===========================================================================
print("Which payment method generates the most revenue?")
spark.sql("""
SELECT payment_type, CAST(SUM(total_amount)AS DECIMAL(18,2)) as revenue
FROM final_trip_analysis
GROUP BY payment_type
ORDER BY revenue DESC
""").show(truncate=False)

## ===========================================================================
## What is the average trip distance?
## ===========================================================================
print("What is the average trip distance?")
spark.sql("""
 SELECT CAST(AVG(trip_distance)AS DECIMAL(18,2)) as avg_trip_distance FROM final_trip_analysis
""").show()

## ===========================================================================
## What is the average trip duration?
## ===========================================================================
print("What is the average trip duration?")
spark.sql("""
 SELECT CONCAT(CAST(AVG(trip_duration)AS DECIMAL(18,2)),' MINS') as avg_trip_duration FROM final_trip_analysis
""").show()

## ===========================================================================
## Which borough generates the highest revenue?
## ===========================================================================
print("Which borough generates the highest revenue?")
spark.sql("""
 SELECT pickup_borough, dropoff_borough, CAST(SUM(total_amount)AS DECIMAL(18,2)) as revenue
 FROM final_trip_analysis
 GROUP BY pickup_borough, dropoff_borough
 ORDER BY revenue DESC
""").show(3)

## ===========================================================================
## What is the average passenger count per trip?
## ===========================================================================
print("What is the average passenger count per trip?")
spark.sql("""
 SELECT CAST(AVG(passenger_count)AS DECIMAL(18,2)) as avg_passenger_count
 FROM final_trip_analysis
 WHERE passenger_count > 0
""").show()

## ===========================================================================
## Which zones have the highest passenger demand?
## ===========================================================================
print("Which zones have the highest passenger demand?")
spark.sql("""
 SELECT pickup_zone, count(pickup_zone) as count
 FROM final_trip_analysis
 GROUP BY pickup_zone
 ORDER BY count DESC
 LIMIT 5
""").show(truncate=False)

## ===========================================================================
## Which zones have the highest fare per distance?
## ===========================================================================
print("Which zones have the highest fare per distance?")
spark.sql("""
SELECT pickup_zone, CAST(SUM(fare_amount)/SUM(trip_distance) AS DECIMAL(18,2)) as fare_per_distance
FROM final_trip_analysis
WHERE trip_distance > 0
GROUP BY pickup_zone
ORDER BY fare_per_distance DESC
LIMIT 5
""").show(truncate=False)

## ===========================================================================
## What is the average fare per kilometer?
## ===========================================================================
print("What is the average fare per kilometer?")
spark.sql("""
          SELECT CAST(AVG(fare_amount/trip_distance)AS DECIMAL(18,2)) as avg_fare_per_kilometer
          FROM final_trip_analysis
          WHERE trip_distance > 0
          """).show()
