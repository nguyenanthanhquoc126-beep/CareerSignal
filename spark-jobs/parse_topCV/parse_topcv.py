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
present=datetime.now()


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

            # Spark + Iceberg + Nessie Catalog
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"
        ),
    )

    # Cho phép Spark SQL dùng MERGE / UPDATE / DELETE của Iceberg
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )

    # =========================================================
    # MINIO
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

    # Tạo catalog tên "nessie"
    .config(
        "spark.sql.catalog.nessie",
        "org.apache.iceberg.spark.SparkCatalog",
    )

    # Catalog nessie sử dụng NessieCatalog
    .config(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )

    # Địa chỉ Nessie Server
    .config(
        "spark.sql.catalog.nessie.uri",
        "http://nessie:19120/api/v1",
    )

    # Branch mặc định
    .config(
        "spark.sql.catalog.nessie.ref",
        "main",
    )

    # Local Nessie không authentication
    .config(
        "spark.sql.catalog.nessie.authentication.type",
        "NONE",
    )

    # Root storage của các Iceberg table
    .config(
        "spark.sql.catalog.nessie.warehouse",
        "s3a://warehouse/",
    )

    # Iceberg dùng Hadoop/S3A để đọc ghi MinIO
    .config(
        "spark.sql.catalog.nessie.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO",
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
    f"s3a://bronze/topcv/{present.year}-{present.month}-{present.day}/"
)

data = (
    data
    .withColumn(
        "job_id",
        F.col("job_id").cast("string")
    )
    .withColumn(
        "salary_parsed",
        apply_salary_parser(
            F.col("salary")
        )
    )
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
data_final = data_final.withColumn(
    "scraped_at",
    F.to_timestamp("scraped_at")
)
data_final.createOrReplaceTempView("topcv_newjobs")
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver;")


spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.topcv (
        job_id STRING,
        job_url STRING,
        title STRING,
        city STRING,
        company_name STRING,
        apply_url STRING,
        verification_level STRING,

        salary_min DOUBLE,
        salary_max DOUBLE,
        salary_currency STRING,
        salary_period STRING,
        salary_parse_error STRING,

        scraped_at TIMESTAMP,

        role_group STRING,
        primary_role STRING,
        secondary_roles ARRAY<STRING>,

        seniority STRING,
        experience_years DOUBLE,
        skills ARRAY<STRING>,
        is_multi_role BOOLEAN,

        parse_status STRING,
        parse_error STRING
    )
    USING iceberg
    LOCATION 's3a://warehouse/silver/topcv'
    TBLPROPERTIES (
        'write.format.default' = 'parquet'
    );
""")

spark.sql("""
    MERGE INTO nessie.silver.topcv AS target
    USING topcv_newjobs AS source
    ON target.job_id = source.job_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")


spark.stop()