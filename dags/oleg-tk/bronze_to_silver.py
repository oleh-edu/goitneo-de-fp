import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))

clean_text_udf = udf(clean_text, StringType())

def bronze_to_silver(spark_config, file_name):
    spark = (SparkSession.builder
        .appName("Bronze to Silver")
        #.master(spark_config["host"])
        .master("local[*]")
        .getOrCreate())

    df = spark.read.parquet(f"./data/bronze/{file_name}")
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, clean_text_udf(df[col_name]))

    df = df.dropDuplicates()
    os.makedirs(f"./data/silver/{file_name}", exist_ok=True)
    df.write.parquet(f"./data/silver/{file_name}", mode="overwrite")
    print(f"File {file_name} saved to Silver zone as parquet.")
    # DEBUG
    df.show()

if __name__ == "__main__":
    spark_config = {
        "host": "spark://217.61.58.159:7077"
    }

    for table in ["athlete_bio", "athlete_event_results"]:
        bronze_to_silver(spark_config, table)
