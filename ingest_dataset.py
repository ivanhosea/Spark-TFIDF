import os
import configparser
from datasets import Dataset
from pyspark.sql import SparkSession

config = configparser.ConfigParser()
config.read('ingest_config.properties')
HDFS_URL = config.get('ingest', 'hdfs_url')

os.environ['JAVA_HOME'] = config.get('ingest', 'java_home')
os.environ["HADOOP_USER_NAME"] = config.get('ingest', 'hadoop_user_name')

file_path = [f"wikipedia/wikipedia-train-00000-000{str(i).zfill(2)}-of-NNNNN.arrow" for i in range(72)]

# Create a SparkSession in local mode
spark = SparkSession.builder \
    .appName("Ingest_to_HDFS") \
    .config("spark.master", "local[*]") \
    .config("spark.hadoop.parquet.block.size", "16777216") \
    .config("spark.hadoop.dfs.blocksize", "16777216") \
    .config("spark.hadoop.dfs.replication", "1") \
    .getOrCreate()

# .master("local[*]")

for fp in file_path:
    df = Dataset.from_file(fp)
    df = spark.createDataFrame(list(df))
    df.coalesce(1).write.format("parquet").mode("append").save(f"hdfs://{HDFS_URL}/wiki2")