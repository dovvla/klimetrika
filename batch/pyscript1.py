from datetime import datetime

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.functions import avg, max, min, month
from pyspark.sql.types import *
from pyspark.sql.functions import col, stddev

conf = (
    SparkConf()
    .setAppName("Spark minIO Test")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
    .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .set("spark.hadoop.fs.s3a.access.key", "admin")
    .set("spark.hadoop.fs.s3a.secret.key", "adminadmin")
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.memory.offHeap.size", "16g")
    .set("spark.memory.offHeap.enabled", "true")
    .set("spark.driver.memory", "4g")
    .set("spark.executor.memory", "4g")
    .set("spark.driver.maxResultSize", "20g")
)
sc = SparkContext(conf=conf).getOrCreate()
sc.setLogLevel("WARN")
sqlContext = SQLContext(sc)


BASE_OUTPUT_PATH = "s3a://transform/"

df_air_quality = sqlContext.read.csv(
    "s3a://raw/data/dataset/air_quality.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("airQuality", StringType(), True),
        ]
    ),
)
df_atmospheric_pressure = sqlContext.read.csv(
    "s3a://raw/data/dataset/atmospheric_pressure.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("atmosphericPressure", FloatType(), True),
        ]
    ),
)
df_humidity = sqlContext.read.csv(
    "s3a://raw/data/dataset/humidity.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("humidity", FloatType(), True),
        ]
    ),
)
df_location = sqlContext.read.csv(
    "s3a://raw/data/dataset/location.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("lat", FloatType(), True),
            StructField("lon", FloatType(), True),
        ]
    ),
)
df_pm_1 = sqlContext.read.csv(
    "s3a://raw/data/dataset/pm_1.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("pm1", FloatType(), True),
        ]
    ),
)
df_pm_10 = sqlContext.read.csv(
    "s3a://raw/data/dataset/pm_10.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("pm10", FloatType(), True),
        ]
    ),
)
df_pm_2_5 = sqlContext.read.csv(
    "s3a://raw/data/dataset/pm_2_5.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("pm25", FloatType(), True),
        ]
    ),
)
df_temperature = sqlContext.read.csv(
    "s3a://raw/data/dataset/temperature.csv",
    header=False,
    schema=StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", FloatType(), True),
        ]
    ),
)


from pyspark.sql.functions import col, count, hour, year
from pyspark.sql.window import Window

windowSpec = Window.partitionBy("deviceId").orderBy("timestamp")


df_combined = (
    df_air_quality.join(df_atmospheric_pressure, ["deviceId", "timestamp"], "outer")
    .join(df_humidity, ["deviceId", "timestamp"], "outer")
    .join(df_location, ["deviceId", "timestamp"], "outer")
    .join(df_pm_1, ["deviceId", "timestamp"], "outer")
    .join(df_pm_10, ["deviceId", "timestamp"], "outer")
    .join(df_pm_2_5, ["deviceId", "timestamp"], "outer")
    .join(df_temperature, ["deviceId", "timestamp"], "outer")
)

df_combined = df_combined.filter(
    col("deviceId").isNotNull() & col("timestamp").isNotNull()
)


total_measurements_df = df_combined.groupBy("deviceId").agg(
    count("*").alias("total_measurements")
)

total_measurements_df.show()
total_measurements_df.write.csv(BASE_OUTPUT_PATH + "total_measurements_df.csv")


df_combined = df_combined.withColumn("hour", hour(df_combined.timestamp))

result = df_combined.groupBy("deviceId", "hour").agg(
    count("*").alias("num_measurements")
)

result.show()
result.write.csv(BASE_OUTPUT_PATH + "df_combined.csv")


max_temperature = (
    df_temperature.select("deviceId", "temperature", "timestamp")
    .groupBy("deviceId")
    .agg(max("temperature").alias("max_temperature"))
    .withColumn("max_temperature", col("max_temperature").cast("float"))
)
result = max_temperature.show()
max_temperature.write.csv(BASE_OUTPUT_PATH + "max_temperature.csv")


min_atmospheric_pressure = (
    df_atmospheric_pressure.select("deviceId", "atmosphericPressure", "timestamp")
    .groupBy("deviceId")
    .agg(min("atmosphericPressure").alias("min_atmospheric_pressure"))
    .withColumn(
        "min_atmospheric_pressure", col("min_atmospheric_pressure").cast("float")
    )
)
result = min_atmospheric_pressure.filter(col("min_atmospheric_pressure") > 0).show()
min_atmospheric_pressure.write.csv(BASE_OUTPUT_PATH + "min_atmospheric_pressure.csv")


avg_pressure_by_device = (
    df_atmospheric_pressure.select("deviceId", "atmosphericPressure")
    .groupBy("deviceId")
    .agg(avg("atmosphericPressure").alias("avg_atmospheric_pressure"))
)
avg_pressure_by_device.write.csv(BASE_OUTPUT_PATH + "avg_pressure_by_device.csv")


stddev_humidity = (
    df_humidity.select("deviceId", "humidity", "timestamp")
    .groupBy("deviceId")
    .agg(stddev("humidity").alias("stddev_humidity"))
    .withColumn("stddev_humidity", col("stddev_humidity").cast("float"))
)
result = stddev_humidity.filter(col("stddev_humidity") > 0).show()
stddev_humidity.write.csv(BASE_OUTPUT_PATH + "stddev_humidity.csv")

df_monthly_temperature_stats = (
    df_temperature.filter(year("timestamp") == 2021)
    .filter("temperature > -100")
    .filter("temperature < 100")
    .groupBy(month("timestamp").alias("month"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
    )
    .orderBy("month")
)
df_monthly_temperature_stats.write.csv(
    BASE_OUTPUT_PATH + "df_monthly_temperature_stats.csv"
)


df_air_quality_monthly = (
    df_air_quality.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("airQuality").alias("avg_air_quality"),
        min("airQuality").alias("min_air_quality"),
        max("airQuality").alias("max_air_quality"),
    )
)
df_air_quality_monthly.write.csv(BASE_OUTPUT_PATH + "df_air_quality_monthly.csv")
df_atmospheric_pressure_monthly = (
    df_atmospheric_pressure.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("atmosphericPressure").alias("avg_atmospheric_pressure"),
        min("atmosphericPressure").alias("min_atmospheric_pressure"),
        max("atmosphericPressure").alias("max_atmospheric_pressure"),
    )
)
df_atmospheric_pressure_monthly.write.csv(
    BASE_OUTPUT_PATH + "df_atmospheric_pressure_monthly.csv"
)

# For df_humidity
df_humidity_monthly = (
    df_humidity.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("humidity").alias("avg_humidity"),
        min("humidity").alias("min_humidity"),
        max("humidity").alias("max_humidity"),
    )
)
df_humidity_monthly.write.csv(BASE_OUTPUT_PATH + "df_humidity_monthly.csv")


# For df_pm_1
df_pm_1_monthly = (
    df_pm_1.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("pm1").alias("avg_pm1"),
        min("pm1").alias("min_pm1"),
        max("pm1").alias("max_pm1"),
    )
)
df_pm_1_monthly.write.csv(BASE_OUTPUT_PATH + "df_pm_1_monthly.csv")


# For df_pm_10
df_pm_10_monthly = (
    df_pm_10.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("pm10").alias("avg_pm10"),
        min("pm10").alias("min_pm10"),
        max("pm10").alias("max_pm10"),
    )
)
df_pm_10_monthly.write.csv(BASE_OUTPUT_PATH + "df_pm_10_monthly.csv")


# For df_pm_2_5
df_pm_2_5_monthly = (
    df_pm_2_5.withColumn("year", year("timestamp"))
    .withColumn("month", month("timestamp"))
    .groupBy("year", "month")
    .agg(
        avg("pm25").alias("avg_pm25"),
        min("pm25").alias("min_pm25"),
        max("pm25").alias("max_pm25"),
    )
)
df_pm_2_5_monthly.write.csv(BASE_OUTPUT_PATH + "df_pm_2_5_monthly.csv")
