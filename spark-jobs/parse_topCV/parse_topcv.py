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
from parse_salary_TOPCV import apply_salary_parser
from parse_job_topCV import parse_partition


run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = f"s3a://silver/topcv/{run_date}"

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
    StructField("role_group", StringType(), True),
    StructField("primary_role", StringType(), True),
    StructField(
        "secondary_roles",
        ArrayType(StringType()),
        False,
    ),
    StructField("seniority", StringType(), False),
    StructField("experience_years", DoubleType(), True),
    StructField(
        "skills",
        ArrayType(StringType()),
        False,
    ),
    StructField("is_multi_role", BooleanType(), False),
    StructField("parse_status", StringType(), False),
    StructField("parse_error", StringType(), True),
])
data=spark.read.parquet(
    "s3a://bronze/2026-7-30/1785403101.108542-TopCV.parquet"
)

data = data.withColumn(
    "salary_parsed",
    apply_salary_parser(
        F.col("salary")
    ),
)
data = (
    data
    .withColumns({
        "salary_min": F.col(
            "salary_parsed.min_amount"
        ),
        "salary_max": F.col(
            "salary_parsed.max_amount"
        ),
        "salary_currency": F.col(
            "salary_parsed.currency"
        ),
        "salary_period": F.col(
            "salary_parsed.period"
        ),
        "salary_type": F.col(
            "salary_parsed.salary_type"
        ),
        "salary_parse_status": F.col(
            "salary_parsed.parse_status"
        ),
        "salary_parser_version": F.col(
            "salary_parsed.parser_version"
        ),
        "salary_parse_error": F.col(
            "salary_parsed.parse_error"
        ),
    })
    .drop("salary_parsed")
)
data_new=data.select(
    "job_id",
    "title",
    "remaining_tags",
    "visible_tags",
    "experience",
)
data_parsed = data_new.repartition(8).rdd.mapPartitions(parse_partition)
parsed_df = spark.createDataFrame(
    data_parsed,
    schema=parsed_schema,
)
parsed_df.cache()
data_to_join=data.select(
    "job_id",
    "job_url",
    "title",
    "city",
    "company_name",
    "apply_url",
    "verification_level",
    "salary_min",
    "salary_max",
    "salary_currency",
    "salary_period",
    "salary_parse_error",
    "scraped_at"
)
data_final=data_to_join.join(
    parsed_df,
    on="job_id",
    how="inner"
)

data_final.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .format("parquet") \
    .save(output_path) 


spark.stop()