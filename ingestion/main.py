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
from pathlib import Path

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

if not bucket_exist(s3_client=s3_client,bucket_name="bronze"):
    s3_client.create_bucket(Bucket='bronze')


transfer_config = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
)


def upload_dataframe(dataframe: pd.DataFrame, bucket: str, object_name: str):
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
if __name__ == "__main__":
    it_viec=career_it_hcm(page=999)
    topcv_job = career_it(page=999)
    df_itviet=pd.DataFrame(it_viec.values())
    df_topcv=pd.DataFrame(topcv_job)

    upload_dataframe(dataframe=df_itviet,bucket="bronze",object_name=object_nameITViec)
    upload_dataframe(dataframe=df_topcv,bucket='bronze',object_name=object_nameTopCV)