"""
NYC Taxi Trip Analytics
-----------------------
Performs business analytics on processed taxi trip data.

Analytics Included:
- Revenue trends
- Passenger demand
- Geographic revenue insights
- Fare efficiency metrics
"""

from dotenv import load_dotenv
from utils.logger import get_logger
import os
load_dotenv()

logger = get_logger(__name__)

def final_analytic(spark,trip_analytics):
    
    dataset_path = os.getenv("dataset_path")
    try:
        logger.info("Loading final trip analysis dataset")
        
        trip_analytics.createOrReplaceTempView("final_trip_analysis")
        
        logger.info("Running revenue analysis")
        
        queries = {
            
            "Total revenue by month":
            """
            SELECT month, FORMAT_NUMBER(sum(total_amount),2) as revenue FROM final_trip_analysis
            GROUP BY month
            ORDER BY month
            """,
            
            "Most popular pickup zone":   
            """
            SELECT pickup_zone, count(pickup_zone) as count FROM final_trip_analysis
            GROUP BY pickup_zone
            ORDER BY count DESC
            LIMIT 5
            """,
            
            "Payment method generating highest revenue":
            """
            SELECT payment_type, FORMAT_NUMBER(SUM(total_amount),2) as revenue
            FROM final_trip_analysis
            GROUP BY payment_type
            ORDER BY revenue DESC
            """,
            
            "Average trip distance":
            """
            SELECT ROUND(AVG(trip_distance),2) as avg_trip_distance 
            FROM final_trip_analysis
            """,
            
            "Average trip duration":
            """
            SELECT CONCAT(ROUND(AVG(trip_duration),2),' MINS') as avg_trip_duration 
            FROM final_trip_analysis
            """,
            
            "borough by generating the highest revenue":
            """
            SELECT pickup_borough, dropoff_borough, FORMAT_NUMBER(SUM(total_amount),2) as revenue
            FROM final_trip_analysis
            GROUP BY pickup_borough, dropoff_borough
            ORDER BY revenue DESC
            """,
            
            "Average passenger count":
            """
            SELECT ROUND(AVG(passenger_count),2) as avg_passenger_count
            FROM final_trip_analysis
            WHERE passenger_count > 0
            """,
            
            "Zones with high passanger demand":
            """
            SELECT pickup_zone, count(pickup_zone) as count
            FROM final_trip_analysis
            GROUP BY pickup_zone
            ORDER BY count DESC
            LIMIT 5
            """,
            
            "Average fare per Distance":
            """
            SELECT pickup_zone, ROUND(SUM(fare_amount)/SUM(trip_distance),2) as fare_per_distance
            FROM final_trip_analysis
            WHERE CAST(trip_distance AS DOUBLE) > 0
            GROUP BY pickup_zone
            ORDER BY fare_per_distance DESC
            LIMIT 5
            """,
            
            "Average fare per KM":
            """
            SELECT ROUND(
                AVG(
                CAST(fare_amount AS DOUBLE)/
                CAST(trip_distance AS DOUBLE)),2) AS avg_fare_per_kilometer
            FROM final_trip_analysis
            WHERE CAST(trip_distance AS DOUBLE) > 0
            """
        }
        
        for title,query in queries.items():
            print(f"\n=== {title} ===")
            spark.sql(query).show(truncate=False)
        
        logger.info("Analytics completed successfully")
    
    except Exception as e:
        logger.error(f"Analytics pipeline failed: {e}")
        
    return 
