from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

import os

load_dotenv()

def extract_data(spark,raw=None, dataset_path=None):

    raw = os.getenv("RAW_DATA")
    dataset_path = os.getenv("dataset_path")

    df = spark.read.csv(raw, header=True)

    df = df.drop("_c0")

    df.coalesce(1).\
        write.\
        mode("overwrite").\
        option("header", True).\
        csv(f"{dataset_path}/bronze")
    
    logger.info("DataSets Extracted To Bronze")

    return df
