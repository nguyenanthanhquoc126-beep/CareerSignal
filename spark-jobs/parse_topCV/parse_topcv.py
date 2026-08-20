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
from logging_config import logging, set_up_log
from parse_salary_TOPCV import PARSER_VERSION, apply_salary_parser
from parse_job_topCV import parse_partition

LOG_COMPONENT = "[Spark][TopCV][driver]"
set_up_log()

run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
present=datetime.now()

logging.info(
    "%s Bắt đầu job chuyển dữ liệu Bronze sang Silver | "
    "run_date=%s | source=%s | target_table=%s | target_location=%s | "
    "ollama_partitions=%s | salary_parser=%s.",
    LOG_COMPONENT,
    run_date,
    f"s3a://bronze/topcv/{present.year}-{present.month}-{present.day}/",
    "nessie.silver.topcv",
    "s3a://warehouse/silver/topcv",
    8,
    PARSER_VERSION,
)


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
logging.info(
    "%s SparkSession đã sẵn sàng | app_name=%s | master=%s | "
    "application_id=%s.",
    LOG_COMPONENT,
    spark.sparkContext.appName,
    spark.sparkContext.master,
    spark.sparkContext.applicationId,
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
logging.info(
    "%s Bắt đầu tạo DataFrame nguồn từ Bronze | source=%s.",
    LOG_COMPONENT,
    f"s3a://bronze/topcv/{present.year}-{present.month}-{present.day}/",
)
data=spark.read.parquet(
    f"s3a://bronze/topcv/{present.year}-{present.month}-{present.day}/"
)
logging.info(
    "%s Đã tạo DataFrame nguồn từ Bronze theo cơ chế lazy của Spark | "
    "columns=%s.",
    LOG_COMPONENT,
    ",".join(data.columns),
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
logging.info(
    "%s Đã cấu hình salary parser | parser=%s | "
    "kết quả lỗi nghiệp vụ được lưu trong salary_parse_error.",
    LOG_COMPONENT,
    PARSER_VERSION,
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
logging.info(
    "%s Đã cấu hình bước phân loại role, seniority và skills bằng Ollama | "
    "partitions=%s | cache=lazy | lỗi từng job được lưu trong parse_error.",
    LOG_COMPONENT,
    8,
)
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
logging.info(
    "%s Đã cấu hình phép join dữ liệu đã parse với dữ liệu job | key=job_id | "
    "join_type=inner.",
    LOG_COMPONENT,
)
data_final = data_final.withColumn(
    "scraped_at",
    F.to_timestamp("scraped_at")
)
data_final.createOrReplaceTempView("topcv_newjobs")
logging.info(
    "%s Đang bảo đảm namespace Iceberg tồn tại | namespace=nessie.silver.",
    LOG_COMPONENT,
)
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver;")
logging.info(
    "%s Namespace Iceberg đã sẵn sàng | namespace=nessie.silver.",
    LOG_COMPONENT,
)


logging.info(
    "%s Đang bảo đảm bảng Iceberg tồn tại | table=%s | location=%s.",
    LOG_COMPONENT,
    "nessie.silver.topcv",
    "s3a://warehouse/silver/topcv",
)
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
logging.info(
    "%s Bảng Iceberg đã sẵn sàng | table=%s | location=%s.",
    LOG_COMPONENT,
    "nessie.silver.topcv",
    "s3a://warehouse/silver/topcv",
)

logging.info(
    "%s Bắt đầu MERGE dữ liệu vào Silver | source_view=topcv_newjobs | "
    "target_table=%s | key=job_id.",
    LOG_COMPONENT,
    "nessie.silver.topcv",
)
spark.sql("""
    MERGE INTO nessie.silver.topcv AS target
    USING topcv_newjobs AS source
    ON target.job_id = source.job_id

    WHEN MATCHED THEN
        UPDATE SET *

    WHEN NOT MATCHED THEN
        INSERT *
""")
logging.info(
    "%s MERGE dữ liệu Silver hoàn tất | target_table=%s | key=job_id.",
    LOG_COMPONENT,
    "nessie.silver.topcv",
)


logging.info("%s Đang dừng SparkSession sau khi job hoàn tất.", LOG_COMPONENT)
spark.stop()
logging.info("%s Đã dừng SparkSession; job kết thúc thành công.", LOG_COMPONENT)
