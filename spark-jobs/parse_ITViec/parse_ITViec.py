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

    # =========================================================
    # THƯ VIỆN
    # =========================================================
    .config(
        "spark.jars.packages",
        (
            # Spark đọc/ghi MinIO bằng S3A
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"

            # Spark làm việc với Iceberg
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
        ),
    )

    # Cho phép Spark SQL dùng MERGE / UPDATE / DELETE của Iceberg
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # =========================================================
    # MINIO - S3A
    # =========================================================
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

    # =========================================================
    # NESSIE / ICEBERG CATALOG
    # =========================================================

    # Tạo một Spark catalog tên "nessie"
    .config(
        "spark.sql.catalog.nessie",
        "org.apache.iceberg.spark.SparkCatalog",
    )

    # Nói cho Iceberg biết catalog này dùng Nessie
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )

    # Địa chỉ Nessie server
    .config(
        "spark.sql.catalog.nessie.uri",
        "http://nessie:19120/api/v1",
    )

    # Làm việc trên branch main
    .config(
        "spark.sql.catalog.nessie.ref",
        "main",
    )

    # Nessie local không dùng authentication
    .config(
        "spark.sql.catalog.nessie.authentication.type",
        "NONE",
    )
    .config(
        "spark.sql.catalog.nessie.warehouse",
        "s3a://warehouse/",
    )

    # Vì code hiện tại đang dùng S3A/Hadoop
    .config(
        "spark.sql.catalog.nessie.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO",
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
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver;")

data_final = data_final.withColumn(
    "scraped_at",
    F.to_timestamp("scraped_at")
)

data_final.createOrReplaceTempView("itviec_newjobs")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.itviec (
        job_id STRING,
        min_salary DOUBLE,
        max_salary DOUBLE,
        currency STRING,
        period STRING,
        parse_status STRING,
        title STRING,
        job_url STRING,
        company_name STRING,
        working_model STRING,
        location STRING,
        skills ARRAY<STRING>,
        benefits ARRAY<STRING>,
        posted_at STRING,
        scraped_at TIMESTAMP
    )
    USING iceberg
    LOCATION 's3a://warehouse/silver/itviec'
    TBLPROPERTIES (
        'write.format.default' = 'parquet'
    );
""")
spark.sql("""
    MERGE INTO nessie.silver.itviec AS target
    USING itviec_newjobs AS source
    ON target.job_id = source.job_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")

spark.stop()