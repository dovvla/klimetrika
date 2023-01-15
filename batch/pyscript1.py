from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import Window, functions as F
from pyspark import SparkConf, SparkContext, SQLContext


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
sqlContext = SQLContext(sc)


def one_asset_to_csv(df: "DataFrame", asset_name: str):
    df.filter(col("asset_title") == asset_name).select(
        "asset_value.Value", "asset_value.Time", "_id", "device_title"
    ).write.options(header="True", delimiter=",").format("csv").save(
        f"s3a://transform/data/{asset_name}.csv", mode="overwrite"
    )


df = sqlContext.read.json("s3a://raw/data/klimerko_raw_data_pp")
asset_titles = df.select("asset_title").distinct().collect()
asset_titles = [v["asset_title"] for v in asset_titles]
for at in asset_titles:
    one_asset_to_csv(df, at)
