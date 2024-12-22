import os
import requests
from pyspark.sql import SparkSession

def download_data(file_name):
    url = f"https://ftp.goit.study/neoversity/{file_name}.csv"
    local_file_path = f"./data/landing/{file_name}.csv"
    os.makedirs("./data/landing", exist_ok=True)

    response = requests.get(url)
    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
        print(f"File {file_name} downloaded successfully.")
    else:
        exit(f"Failed to download {file_name}. Status code: {response.status_code}")

def save_to_bronze(spark_config, file_name):
    spark = (SparkSession.builder \
        .appName("Landing to Bronze") \
        #.master(spark_config["host"])
        .master("local[*]")
        .getOrCreate())

    df = spark.read.csv(f"./data/landing/{file_name}.csv", header=True, inferSchema=True)
    os.makedirs(f"./data/bronze/{file_name}", exist_ok=True)
    df.write.parquet(f"./data/bronze/{file_name}", mode="overwrite")
    print(f"File {file_name} saved to Bronze zone as parquet.")
    # DEBUG
    df.show()

if __name__ == "__main__":
    spark_config = {
        "host": "spark://217.61.58.159:7077"
    }

    for table in ["athlete_bio", "athlete_event_results"]:
        download_data(table)
        save_to_bronze(spark_config, table)
