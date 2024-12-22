import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp

def silver_to_gold(spark_config):
    spark = (SparkSession.builder
        .appName("Silver to Gold")
        #.master(spark_config["host"])
        .master("local[*]")
        .getOrCreate())

    # Reading tables
    bio_df = spark.read.parquet("./data/silver/athlete_bio")
    results_df = spark.read.parquet("./data/silver/athlete_event_results")

    # Type conversion
    bio_df = bio_df.withColumn("height", bio_df["height"].cast("float")) \
                   .withColumn("weight", bio_df["weight"].cast("float"))

    # Merge tables
    joined_df = bio_df.join(results_df, on="athlete_id", how="inner") \
        .select(
            results_df["sport"],            # З results_df
            bio_df["sex"],                  # З bio_df
            bio_df["country_noc"].alias("bio_country_noc"),
            results_df["country_noc"].alias("results_country_noc"),
            results_df["medal"],            # З results_df
            bio_df["height"],               # З bio_df
            bio_df["weight"]                # З bio_df
        )

    # Calculating average values
    agg_df = joined_df.groupBy("sport", "medal", "sex", "bio_country_noc") \
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight")
        ) \
        .withColumn("timestamp", current_timestamp())

    # Recording data in the Gold Zone
    os.makedirs("./data/gold/avg_stats", exist_ok=True)
    agg_df.write.parquet("./data/gold/avg_stats", mode="overwrite")
    print("Gold zone data saved to avg_stats.")
    # DEBUG
    agg_df.show()

if __name__ == "__main__":
    spark_config = {
        "host": "spark://217.61.58.159:7077"
    }

    silver_to_gold(spark_config)
