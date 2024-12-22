import os
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4,'
    'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,'
    'com.mysql:mysql-connector-j:8.0.32 pyspark-shell'
)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType


class AthleteDataProcessor:
    def __init__(self, mysql_config, kafka_config, spark_config):
        # Initializing a SparkSession with the right parameters
        self.spark = (SparkSession.builder
                        .appName("OLEG: Athlete Data Processing")
                        #.master(spark_config["host"])
                        .master("local[*]")
                        .config("spark.executor.memory", "512m")
                        .config("spark.driver.memory", "512m")
                        .config("spark.executor.cores", "1")
                        .getOrCreate())

        # Setting a global directory for checkpoints
        self.spark.conf.set("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints")

        # Adaptive query execution is not supported for streaming DataFrames/Datasets. Therefore, we exclude it.
        self.spark.conf.set("spark.sql.adaptive.enabled", "false")

        self.mysql_config = mysql_config
        self.kafka_config = kafka_config

    def read_mysql_table(self, table_name):
        """
        Reading data from a MySQL table.
        """
        df = (self.spark.read
            .format("jdbc")
            .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", self.mysql_config['user'])
            .option("password", self.mysql_config['password'])
            .load())
        df.show() # FOR DEBUG ONLY
        return df

    def filter_bio_data(self, athlete_bio_df):
        """
        Filter data by missing height and weight values.
        """
        return athlete_bio_df.filter(
            col("height").isNotNull() & col("weight").isNotNull() &
            col("height").cast("float").isNotNull() & col("weight").cast("float").isNotNull()
        )

    def write_to_kafka(self, df, topic):
        """
        Recording data in Kafka.
        """
        query = (df.selectExpr("to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['host']) \
                .option("kafka.sasl.mechanism", "PLAIN") \
                .option("kafka.security.protocol", "SASL_PLAINTEXT") \
                .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self.kafka_config["user"]}" password="{self.kafka_config["password"]}";') \
                .option("topic", topic) \
                .save())
        return query

    def read_from_kafka(self, topic, schema):
        """
        Reading data from Kafka and converting it to a DataFrame.
        """
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_config['host']) \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self.kafka_config["user"]}" password="{self.kafka_config["password"]}";') \
            .option("subscribe", topic) \
            .load()

        return kafka_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

    def calculate_aggregations(self, joined_df):
        """
        Calculation of average values.
        """
        return joined_df.groupBy("sport", "medal", "sex", "country_noc") \
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight")
            ) \
            .withColumn("timestamp", current_timestamp())

    def write_stream_to_kafka(self, df, topic):
        """
        Data streaming in Kafka.
        """
        stream = (df.selectExpr("to_json(struct(*)) AS value")
                .writeStream
                .format("kafka")
                .option("kafka.bootstrap.servers", self.kafka_config['host'])
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.security.protocol", "SASL_PLAINTEXT")
                .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{self.kafka_config["user"]}" password="{self.kafka_config["password"]}";') \
                .option("topic", topic)
                .outputMode("update")
                .start())
        return stream

    def write_stream_to_mysql(self, df, table_name):
        """
        Streaming data to MySQL.
        """
        def write_to_db(batch_df, _):
            batch_df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}/{self.mysql_config['database']}") \
                .option("dbtable", table_name) \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .mode("append") \
                .save()

        stream = (df.writeStream \
                    .foreachBatch(write_to_db) \
                    .outputMode("update") \
                    .start())
        return stream


if __name__ == "__main__":
    # MySQL configuration
    mysql_config = {
        "host": "217.61.57.46",
        "port": "3306",
        # "database": "neo_data",
        "database": "olympic_dataset",
        "user": "neo_data_admin",
        "password": "Proyahaxuqithab9oplp"
    }

    # Kafka configuration
    kafka_config = {
        "host": "77.81.230.104:9092",
        "user": "admin",
        "password": "VawEzo1ikLtrA8Ug8THa"
    }

    # Spark configuration
    spark_config = {
        "host": "spark://217.61.58.159:7077"
    }

    processor = AthleteDataProcessor(mysql_config, kafka_config, spark_config)

    # Reading data
    athlete_bio_df = processor.read_mysql_table("athlete_bio")
    athlete_bio_df = processor.filter_bio_data(athlete_bio_df)

    athlete_event_results_df = processor.read_mysql_table("athlete_event_results")
    processor.write_to_kafka(athlete_event_results_df, "athlete_event_results")

    # Scheme for a Kafka topic
    event_schema = StructType([
        StructField("athlete_id", StringType(), True),
        StructField("sport", StringType(), True),
        StructField("medal", StringType(), True),
        StructField("timestamp", StringType(), True),
    ])

    # Reading data from Kafka
    kafka_event_df = processor.read_from_kafka("athlete_event_results", event_schema)

    # Data merging
    joined_df = kafka_event_df.join(athlete_bio_df, on="athlete_id", how="inner")

    # Calculating average values
    agg_df = processor.calculate_aggregations(joined_df)

    # Streaming in Kafka and MySQL
    kafka_query = processor.write_stream_to_kafka(agg_df, "athlete_enriched_agg")
    mysql_query = processor.write_stream_to_mysql(agg_df, "athlete_enriched_agg")

    # Maintaining the program
    if kafka_query is not None:
        kafka_query.awaitTermination()
    else:
        print("Streaming query for Kafka did not start successfully.")

    if mysql_query is not None:
        mysql_query.awaitTermination()
    else:
        print("Streaming query for MySQL did not start successfully.")