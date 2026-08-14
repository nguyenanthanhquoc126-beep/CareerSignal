from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    DoubleType,
    StringType,
    ArrayType
)
from datetime import datetime, timezone
from pyspark import StorageLevel
import json
import math
import requests
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
from parse_job_ITviec import parse_partition

run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = f"s3a://silver/ITViec/{run_date}"

spark = (
    SparkSession.builder
    .appName("CareerSignal")
    # THƯ VIỆN ĐỂ SPARK ĐỌC S3/MINIO
    .config(
        "spark.jars.packages",
        (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    )
    # CẤU HÌNH MINIO
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000",
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["MINIO_USER"],
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["MINIO_PASSWORD"],
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true",
    )
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false",
    )
    .config(
        "spark.hadoop.fs.s3a.endpoint.region",
        "us-east-1",
    )
    .getOrCreate()
)
parsed_schema = StructType([
    StructField("job_id", StringType(), False),
    StructField("min_salary", DoubleType(), True),
    StructField("max_salary", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("period", StringType(), True),
    StructField("parse_status", StringType(), False),
    ])

data=spark.read.parquet("s3a://bronze/2026-7-30/1785403101.108542-ITViec.parquet")

data_salary=data.select("job_id","salary")

data_new=data_salary.repartition(8).rdd.mapPartitions(parse_partition)

dataframe=spark.createDataFrame(
    data_new,
    parsed_schema
)
data_to_join=data.select(
    "job_id",
    "title",
    "job_url",
    "company_name",
    "working_model",
    "location",
    "skills",
    "benefits",
    "posted_at",
    "scraped_at"
)

data_final=dataframe.join(
    data_to_join,
    on="job_id",
    how="inner"
)

data_final.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .format("parquet") \
    .save(output_path) 

spark.stop()