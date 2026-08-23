from pyspark.sql import SparkSession

from utils.logger import get_logger
logger = get_logger(__name__)

def get_spark_session():
    return (
        SparkSession\
        .builder\
        .appName("NYC Data Pipeline")\
        .config("spark.sql.warehouse.dir", "spark-warehouse") \
        .master('local[*]')\
        .getOrCreate()
        )
        