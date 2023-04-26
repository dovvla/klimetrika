from pyspark.sql.functions import from_json, window, avg
from pyspark.sql.types import StructType, StringType, TimestampType, FloatType
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


def foreach_batch(df, epoch_id):
    df.show()
    df.write.format("mongo").mode("overwrite").save()


def foreach_batch_alarm(df, epoch_id):
    print(
        f"ALARM: Average temperature below 20 in {df.select('city').distinct().collect()}"
    )
    df.write.format("mongo").option(
        "uri",
        "mongodb://root:example@mongo:27017/streaming_data.alarms?authSource=admin",
    ).mode("overwrite").save()


spark = (
    SparkSession.builder.appName("Readings conusmer")
    .config(
        "spark.mongodb.output.uri",
        "mongodb://root:example@mongo:27017/streaming_data.aggs?authSource=admin",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

readingSchema = (
    StructType()
    .add("deviceId", StringType())
    .add("time", TimestampType())
    .add("temperature", FloatType())
    .add("humidity", FloatType())
    .add("pressure", FloatType())
    .add("pm1", FloatType())
    .add("pm2", FloatType())
    .add("pm10", FloatType())
    .add("city", StringType())
)
readings = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "test-topic")
    .option("startingOffsets", "earliest")
    .load()
)

readings = readings.withColumn(
    "data", from_json(readings.value.cast(StringType()), readingSchema)
)


aggs_all_query = readings.groupBy(window("timestamp", "30 seconds")).agg(
    avg("data.pm1").alias("avg_pm1"),
    avg("data.pm2").alias("avg_pm2"),
    avg("data.pm10").alias("avg_pm10"),
    avg("data.pressure").alias("avg_pressure"),
    avg("data.humidity").alias("avg_humidity"),
    avg("data.temperature").alias("avg_temperature"),
    max("data.pm1").alias("max_pm1"),
    max("data.pm2").alias("max_pm2"),
    max("data.pm10").alias("max_pm10"),
    max("data.pressure").alias("max_pressure"),
    max("data.humidity").alias("max_humidity"),
    max("data.temperature").alias("max_temperature"),
    min("data.pm1").alias("min_pm1"),
    min("data.pm2").alias("min_pm2"),
    min("data.pm10").alias("min_pm10"),
    min("data.pressure").alias("min_pressure"),
    min("data.humidity").alias("min_humidity"),
    min("data.temperature").alias("min_temperature"),
)


aggs_all_query.writeStream.foreachBatch(foreach_batch).outputMode("complete").start()

alarm_query = (
    readings.groupBy(window("timestamp", "30 seconds"), "data.city")
    .agg(avg("data.temperature").alias("avg_temperature"))
    .filter(col("avg_temperature") < 20)
    .writeStream.outputMode("complete")
    .foreachBatch(foreach_batch_alarm)
    .start()
)

prev_avg_temperature = {}


def check_temperature_change(df, epoch_id):
    for row in df.collect():
        city = row.city
        curr_avg_temperature = row.avg_temperature
        if city in prev_avg_temperature:
            prev_avg_temperature[city] = curr_avg_temperature
            temperature_change = abs(curr_avg_temperature - prev_avg_temperature[city])
            if temperature_change >= 10:
                print(
                    f"ALARM: Temperature in {city} changed by {temperature_change} in the last window."
                )
                df.write.format("mongo").option(
                    "uri",
                    "mongodb://root:example@mongo:27017/streaming_data.alarmdiffs?authSource=admin",
                ).mode("overwrite").save()
        else:
            prev_avg_temperature[city] = curr_avg_temperature


alarm_diff_query = (
    readings.groupBy(window("time", "30 seconds"), "data.city")
    .agg(avg("data.temperature").alias("avg_temperature"))
    .writeStream.outputMode("complete")
    .foreachBatch(check_temperature_change)
    .start()
)


spark.streams.awaitAnyTermination()
