import os
import pendulum

from airflow.sdk import dag
from airflow.providers.ssh.operators.ssh import SSHOperator



PROJECT_ROOT = os.environ["CAREER_SIGNAL_ROOT"]


INGESTION_COMMAND = f"""
cd "{PROJECT_ROOT}" && \
DISPLAY="${{DISPLAY:-:0}}" "{PROJECT_ROOT}/.venv/bin/python" -m ingestion.main
"""


SPARK_ITVIEC_COMMAND = f"""
cd "{PROJECT_ROOT}" && \
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.jars.ivy=/tmp/.ivy2-itviec \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --py-files /opt/spark-jobs/parse_ITViec/parse_job_ITviec.py \
  /opt/spark-jobs/parse_ITViec/parse_ITViec.py
"""


SPARK_TOPCV_COMMAND = f"""
cd "{PROJECT_ROOT}" && \
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 \
  --conf spark.jars.ivy=/tmp/.ivy2-topcv \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --py-files /opt/spark-jobs/parse_topCV/parse_job_topCV.py,/opt/spark-jobs/parse_topCV/parse_salary_TOPCV.py \
  /opt/spark-jobs/parse_topCV/parse_topcv.py
"""


DBT_BUILD_COMMAND = f"""
cd "{PROJECT_ROOT}" && \
"{PROJECT_ROOT}/.venv/bin/dbt" build \
  --project-dir "{PROJECT_ROOT}/Careersignal_dbt" \
  --profiles-dir "{PROJECT_ROOT}/Careersignal_dbt"
"""


@dag(
    dag_id="career_signal_spark_pipeline",
    start_date=pendulum.datetime(
        2026,
        8,
        21,
        tz="Asia/Ho_Chi_Minh",
    ),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["career-signal", "spark"],
)
def career_signal_spark_pipeline():

    ingestion = SSHOperator(
        task_id="ingestion",
        ssh_conn_id="pipeline_server",
        command=INGESTION_COMMAND,
        cmd_timeout=None,
        do_xcom_push=False
    )

    spark_itviec = SSHOperator(
        task_id="spark_itviec",
        ssh_conn_id="pipeline_server",
        command=SPARK_ITVIEC_COMMAND,
        cmd_timeout=None,
        do_xcom_push=False
    )

    spark_topcv = SSHOperator(
        task_id="spark_topcv",
        ssh_conn_id="pipeline_server",
        command=SPARK_TOPCV_COMMAND,
        cmd_timeout=None,
        do_xcom_push=False
    )

    dbt_build = SSHOperator(
        task_id="dbt_build",
        ssh_conn_id="pipeline_server",
        command=DBT_BUILD_COMMAND,
        cmd_timeout=None,
        do_xcom_push=False
    )

    ingestion >> spark_itviec >> spark_topcv >> dbt_build


career_signal_spark_pipeline()
