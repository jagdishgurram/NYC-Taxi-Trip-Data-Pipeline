from dotenv import load_dotenv
import os

load_dotenv()

def extract_data(spark):

    raw = os.getenv("RAW_DATA")
    bronze_path = os.getenv("bronze")

    df = spark.read.csv(raw, header=True, inferSchema=True)

    print("Data Extracted Successfully")

    df = df.drop("_c0")

    df.coalesce(1).\
        write.\
        mode("overwrite").\
        option("header", True).\
        parquet(bronze_path)

    print("Data Stored to Bronze Layer")

    return df
