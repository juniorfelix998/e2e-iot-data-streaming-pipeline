from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    IntegerType,
)

from config import configuration


def main():
    spark = (
        SparkSession.builder.appName("IotStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    # log level to minimize console output
    spark.sparkContext.setLogLevel("WARN")

    # create vehicle schema

    vehicleSchema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuel_type", StringType(), True),
        ]
    )

    trafficSchema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("camera_id", StringType(), True),
            StructField("snapshot", StringType(), True),
        ]
    )

    gpsSchema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vehicle_type", StringType(), True),
        ]
    )

    weatherSchema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("air_quality_index", DoubleType(), True),
        ]
    )

    emergencySchema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("incident_id", StringType(), True),
            StructField("type", StringType(), True),
            StructField("location", StringType(), True),
            StructField("status", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )

    def read_kafka_topic(topic, schema):
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", "2 minutes")
        )

    def stream_writer(input: DataFrame, check_point_folder, output):
        return (
            input.writeStream.format("parquet")
            .option("checkpointLocation", check_point_folder)
            .option("path", output)
            .outputMode("append")
            .start()
        )

    vehicle_df = read_kafka_topic("vehicle_data", vehicleSchema).alias("vehicle")
    gps_df = read_kafka_topic("gps_data", gpsSchema).alias("gps")
    traffic_df = read_kafka_topic("traffic_data", trafficSchema).alias("traffic")
    weather_df = read_kafka_topic("weather_data", weatherSchema).alias("weather")
    emergency_df = read_kafka_topic("emergency_data", emergencySchema).alias(
        "emergency"
    )

    # join data frames

    query_1 = stream_writer(
        vehicle_df,
        "s3a://spark-streaming-data-felix/checkpoints/vehicle_data",
        "s3a://spark-streaming-data-felix/data/vehicle_data",
    )
    query_2 = stream_writer(
        gps_df,
        "s3a://spark-streaming-data-felix/checkpoints/gps_data",
        "s3a://spark-streaming-data-felix/data/gps_data",
    )
    query_3 = stream_writer(
        traffic_df,
        "s3a://spark-streaming-data-felix/checkpoints/traffic_data",
        "s3a://spark-streaming-data-felix/data/traffic_data",
    )
    query_4 = stream_writer(
        emergency_df,
        "s3a://spark-streaming-data-felix/checkpoints/emergency_data",
        "s3a://spark-streaming-data-felix/data/emergency_data",
    )
    query_5 = stream_writer(
        weather_df,
        "s3a://spark-streaming-data-felix/checkpoints/weather_data",
        "s3a://spark-streaming-data-felix/data/weather_data",
    )

    query_5.awaitTermination()


if __name__ == "__main__":
    main()
