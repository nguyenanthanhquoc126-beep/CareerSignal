from ingestion.topcv import career_it
from  ingestion.ITViec import career_it_hcm 
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import boto3 
from botocore.config import Config
from boto3.s3.transfer import TransferConfig
import tempfile
from dotenv import load_dotenv
from logging_config import logging, set_up_log
from pathlib import Path

set_up_log()

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

present=datetime.now()

object_nameITViec=f"itviec/{present.year}-{present.month}-{present.day}/{present.timestamp()}-ITViec.parquet"
object_nameTopCV=f"topcv/{present.year}-{present.month}-{present.day}/{present.timestamp()}-TopCV.parquet"


s3_client=boto3.client(
    "s3",
    endpoint_url="http://172.22.176.1:9000",
    aws_access_key_id= os.environ['MINIO_USER'],
    aws_secret_access_key=os.environ['MINIO_PASSWORD'],
    region_name='us-east-1',
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
)


def bucket_exist(s3_client,bucket_name):
    response=s3_client.list_buckets()
    buckets_name=[
        bucket['Name'] for bucket in response['Buckets']
    ]
    return bucket_name in buckets_name

logging.info("[MinIO] Đang kiểm tra bucket bronze.")
if not bucket_exist(s3_client=s3_client,bucket_name="bronze"):
    logging.info("[MinIO] Bucket bronze chưa tồn tại; đang tạo bucket.")
    s3_client.create_bucket(Bucket='bronze')
    logging.info("[MinIO] Đã tạo bucket bronze.")
else:
    logging.info("[MinIO] Bucket bronze đã tồn tại.")


transfer_config = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
)


def upload_dataframe(dataframe: pd.DataFrame, bucket: str, object_name: str):
    logging.info(
        "[MinIO] Đang chuyển %s dòng, %s cột thành Parquet và upload tới "
        "s3://%s/%s.",
        len(dataframe),
        len(dataframe.columns),
        bucket,
        object_name,
    )

    with tempfile.NamedTemporaryFile(suffix=".parquet") as temp_file:
        dataframe.to_parquet(
            temp_file.name,
            index=False,
            engine="pyarrow"
        )
        s3_client.upload_file(
            Filename=temp_file.name,
            Bucket=bucket,
            Key = object_name,
            ExtraArgs={
                "ContentType":"application/vnd.apache.parquet"
            },
            Config=transfer_config,
        )

    logging.info(
        "[MinIO] Đã upload thành công %s dòng tới s3://%s/%s.",
        len(dataframe),
        bucket,
        object_name,
    )


if __name__ == "__main__":
    logging.info("[Pipeline] Bắt đầu pipeline ingestion.")

    logging.info("[Pipeline] Bắt đầu crawl ITViec.")
    it_viec=career_it_hcm(page=999)
    logging.info(
        "[Pipeline] Crawl ITViec hoàn tất: %s job.",
        len(it_viec),
    )

    logging.info("[Pipeline] Bắt đầu crawl TopCV.")
    topcv_job = career_it(page=999)
    logging.info("[Pipeline] TopCV trả về %s job đã xử lý.", len(topcv_job))

    df_itviet=pd.DataFrame(it_viec.values())
    df_topcv=pd.DataFrame(topcv_job)

    upload_dataframe(dataframe=df_itviet,bucket="bronze",object_name=object_nameITViec)
    upload_dataframe(dataframe=df_topcv,bucket='bronze',object_name=object_nameTopCV)

    logging.info("[Pipeline] Kết thúc pipeline ingestion.")
