from dotenv import load_dotenv
import os

load_dotenv()

def extract_data(spark):

    raw = os.getenv("RAW_DATA")
    dataset_path = os.getenv("dataset_path")

    df = spark.read.csv(raw, header=True, inferSchema=True)

    df = df.drop("_c0")

    df.coalesce(1).\
        write.\
        mode("overwrite").\
        option("header", True).\
        parquet(f"{dataset_path}/bronze")

    return df
