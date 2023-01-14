from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import Window, functions as F
from pyspark import SparkConf, SparkContext, SQLContext


conf = (
    SparkConf()
    .setAppName("Spark minIO Test")
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
# print(sc.wholeTextFiles('s3a://raw/data/klimerko_raw_data').collect())
# df = sqlContext.read.csv("s3a://raw/data/decazavazduh.csv", header=True)
df = sqlContext.read.json("s3a://raw/data/klimerko_raw_data_pp")
df.printSchema()
df.show()
