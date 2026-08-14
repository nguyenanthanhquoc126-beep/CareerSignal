# CareerSignal — Kiến trúc và kế hoạch thực hiện

## 1. Mục tiêu kiến trúc

CareerSignal thu thập dữ liệu việc làm từ ITviec và TopCV, lưu dữ liệu thô vào
lớp Bronze, chuẩn hóa bằng Spark, xây dựng bảng phân tích bằng dbt/Trino và gửi
thông báo từ các bảng Gold.

Hai môi trường dùng cùng một luồng logic nhưng khác object storage:

- Local/dev: crawler ghi thẳng dữ liệu Bronze vào MinIO.
- Production: crawler ghi dữ liệu Bronze vào Google Cloud Storage (GCS).
- CSV chỉ dùng để debug hoặc kiểm tra thủ công, không phải đầu ra chính của
  pipeline.

Không để crawler phụ thuộc trực tiếp vào một nhà cung cấp storage. Crawler chỉ
gọi interface `BronzeWriter`; `dev.yaml` chọn `MinioBronzeWriter`, còn
`prod.yaml` chọn `GCSBronzeWriter`.

## 2. Luồng dữ liệu

```text
ITviec / TopCV
       │
       ▼
Playwright crawler + parser
       │
       ▼
BronzeWriter
       ├── Local: MinIO (S3-compatible)
       └── Prod : GCS
       │
       ▼
Bronze JSON/JSONL (append-only, có metadata lần chạy)
       │
       ▼
Spark: kiểm tra schema, chuẩn hóa, loại trùng
       │
       ▼
Silver Parquet/Iceberg trên MinIO hoặc GCS
       │
       ▼
Trino + dbt
       │
       ▼
Gold tables: salary metrics, skill trends, hot jobs
       │
       ▼
Discord/reporting

Airflow điều phối toàn bộ các bước và lưu trạng thái từng lần chạy.
```

## 3. Quy ước lưu dữ liệu trên object storage

Bucket local mặc định: `career-signal`.

```text
s3://career-signal/
├── bronze/
│   └── jobs/
│       ├── source=itviec/
│       │   └── ingestion_date=YYYY-MM-DD/
│       │       └── run_id=<uuid>/part-00001.jsonl
│       └── source=topcv/
│           └── ingestion_date=YYYY-MM-DD/
│               └── run_id=<uuid>/part-00001.jsonl
├── silver/
│   └── jobs/
└── gold/
    ├── salary_metrics/
    ├── skill_trends/
    └── hot_jobs/
```

Mỗi record Bronze tối thiểu phải có:

```text
schema_version
source
run_id
ingested_at
source_page
source_url
job_id
payload
```

Trong đó `payload` chứa các feature crawler bóc được. Bronze phải append-only:
không sửa và không ghi đè dữ liệu cũ. Nếu cần chạy lại, tạo `run_id` mới. Có thể
lưu thêm response gốc vào `bronze/raw_responses/` để debug hoặc replay.

## 4. Cấu trúc thư mục mục tiêu

```text
datalens-cloud/
│
├── terraform/                                  # Phase 6
│   ├── modules/
│   │   ├── gcs/                                # Task 6.1
│   │   ├── iam/                                # Task 6.2
│   │   ├── secret_manager/                     # Task 6.3
│   │   ├── cloud_function/                     # Task 6.4
│   │   ├── compute_engine/                     # Task 6.5
│   │   └── monitoring/                         # Task 7.1
│   ├── main.tf                                 # Task 6.6
│   ├── variables.tf                            # Task 6.7
│   └── outputs.tf                              # Task 6.8
│
├── ingestion/                                  # Phase 1
│   ├── crawler/
│   │   ├── __init__.py                         # Task 1.1
│   │   ├── models.py                           # Task 1.2
│   │   ├── itviec_client.py                    # Task 1.3
│   │   ├── topcv_client.py                     # Task 1.4
│   │   └── checkpoint.py                       # Task 1.7
│   ├── storage/
│   │   ├── __init__.py                         # Task 1.1
│   │   ├── bronze_writer.py                    # Task 1.5
│   │   ├── minio_writer.py                     # Task 1.6
│   │   └── gcs_writer.py                       # Task 6.10
│   ├── main.py                                 # Task 1.8
│   ├── requirements.txt                        # Task 1.9
│   └── tests/
│       ├── test_crawler.py                     # Task 1.10
│       └── test_minio_writer.py                # Task 1.10
│
├── spark_jobs/                                 # Phase 2
│   ├── bronze_to_silver/
│   │   ├── schema.py                           # Task 2.3
│   │   └── job.py                              # Task 2.4
│   ├── common/
│   │   ├── spark_session.py                    # Task 2.1
│   │   └── object_storage.py                   # Task 2.2
│   └── tests/
│       └── test_bronze_to_silver.py            # Task 2.5
│
├── dbt_project/                                # Phase 3
│   ├── models/
│   │   ├── staging/
│   │   │   ├── _sources.yml                    # Task 3.3
│   │   │   └── stg_jobs.sql                    # Task 3.4
│   │   └── marts/
│   │       ├── gold_salary_metrics.sql         # Task 3.5
│   │       ├── gold_skill_trends.sql           # Task 3.6
│   │       └── _marts.yml                      # Task 3.7
│   ├── tests/singular/
│   │   └── assert_salary_positive.sql          # Task 3.8
│   ├── macros/                                 # Task 3.9
│   ├── dbt_project.yml                         # Task 3.1
│   └── profiles.yml.example                    # Task 3.2
│
├── serving/                                    # Phase 4
│   ├── trino/
│   │   ├── catalog/
│   │   │   └── iceberg.properties              # Task 0.5
│   │   └── queries/
│   │       ├── daily_market_trend.sql          # Task 4.1
│   │       └── hot_job_alert.sql               # Task 4.2
│   └── notifications/
│       ├── report_formatter.py                 # Task 4.3
│       ├── discord_bot.py                      # Task 4.4
│       └── tests/test_report_formatter.py      # Task 4.5
│
├── orchestration/                              # Phase 5
│   ├── dags/datalens_pipeline_dag.py           # Task 5.1
│   ├── plugins/operators/
│   │   └── trino_discord_operator.py           # Task 5.2
│   └── tests/test_dag.py                       # Task 5.3
│
├── security/
│   └── secret_manager_setup.md                 # Task 6.11
│
├── configs/
│   ├── dev.yaml                                # Task 0.3
│   └── prod.yaml                               # Task 6.9
│
├── scripts/
│   ├── local_dev_setup.sh                      # Task 0.6
│   ├── smoke_test_local.sh                     # Task 0.8
│   ├── deploy_cloud_function.sh                # Task 6.12
│   └── deploy_dag.sh                           # Task 6.12
│
├── docker/
│   ├── docker-compose.yml                      # Task 0.4
│   └── minio/
│       └── init-buckets.sh                     # Task 0.4
│
├── docs/
│   ├── architecture.md                         # Task 7.2
│   └── data_dictionary.md                      # Task 7.3
│
├── .env.example                                # Task 0.3
├── .gitignore                                  # Task 0.1
├── pyproject.toml                              # Task 0.2
├── requirements.txt                            # Task 0.7
└── README.md                                   # Task 7.4
```

## 5. Thứ tự task tổng quát

```text
Phase 0 — Môi trường local và MinIO
Task 0.1 → Task 0.8

Phase 1 — Crawler ghi thẳng Bronze vào MinIO
Task 1.1 → Task 1.10

Phase 2 — Spark Bronze → Silver
Task 2.1 → Task 2.5

Phase 3 — dbt Silver → Gold
Task 3.1 → Task 3.10

Phase 4 — Trino query và Discord
Task 4.1 → Task 4.5

Phase 5 — Airflow orchestration
Task 5.1 → Task 5.3

Phase 6 — Deploy GCP và thay MinIO bằng GCS
Task 6.1 → Task 6.13

Phase 7 — Monitoring và tài liệu
Task 7.1 → Task 7.4
```

Quan hệ phụ thuộc quan trọng:

```text
0.4 MinIO local
  └── 1.5 BronzeWriter
        └── 1.6 MinioBronzeWriter
              └── 1.8 ingestion main
                    └── 2.4 Bronze → Silver
                          └── 3.x dbt Gold
                                └── 4.x reporting
                                      └── 5.x Airflow

Sau khi local chạy ổn:
6.1 GCS + 6.2 IAM + 6.3 Secret Manager
  └── 6.10 GCSBronzeWriter
        └── 6.13 kiểm thử chuyển môi trường
```

## 6. Nội dung chi tiết từng task

### Phase 0 — Môi trường local và MinIO

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 0.1 | Tạo `.gitignore` cho `.env`, credential, output CSV, log, checkpoint, `.venv`, `__pycache__`, Spark warehouse và volume local. | Tuyệt đối không commit access key, secret key, cookie đăng nhập hoặc session ITviec/TopCV. | `git status` không hiển thị secret hay file dữ liệu sinh ra. |
| 0.2 | Tạo `pyproject.toml`, cấu hình Python version, formatter, linter và pytest. | Khóa version tương thích giữa Playwright, PySpark, MinIO SDK và Google Cloud SDK. | Cài dependency và chạy kiểm tra code bằng một lệnh. |
| 0.3 | Tạo `.env.example` và `configs/dev.yaml` chứa tên biến, MinIO endpoint, bucket và prefix; giá trị secret thật chỉ nằm trong `.env`. | Phân biệt endpoint từ host (`localhost:9000`) và endpoint giữa container (`minio:9000`). Không ghi secret thật vào YAML. | App đọc được config local và báo lỗi rõ ràng khi thiếu biến. |
| 0.4 | Tạo Docker Compose gồm MinIO và container `minio-init`; expose API `9000`, Console `9001`; script init tạo bucket `career-signal`. Có thể thêm PostgreSQL/catalog service, Trino và Airflow theo nhu cầu local. | MinIO là object storage tương thích S3, không phải filesystem dùng chung. `minio-init` phải idempotent: chạy lại không lỗi nếu bucket đã tồn tại. Gắn named volume để dữ liệu không mất khi container restart. | `docker compose up -d` chạy healthy, đăng nhập được Console và bucket được tạo tự động. |
| 0.5 | Cấu hình Trino Iceberg catalog đọc MinIO. Bật native S3 filesystem, khai báo endpoint, region, path-style và metadata catalog. | MinIO chỉ giữ object; Iceberg vẫn cần catalog metadata như JDBC, REST, Nessie hoặc Hive Metastore. Với Trino mới dùng `fs.s3.enabled=true` và nhóm `s3.*`, không dùng cấu hình legacy `hive.s3.*`. | Trino tạo được schema/table thử nghiệm trên MinIO và đọc lại được dữ liệu. |
| 0.6 | Viết `local_dev_setup.sh` để kiểm tra Docker, tạo `.env` từ template nếu thiếu, khởi động stack và cài Playwright browser. | Script phải chạy lặp lại an toàn, không tự ghi đè `.env` đang có. | Máy mới có thể dựng môi trường local bằng một lệnh. |
| 0.7 | Tạo `requirements.txt` cấp root cho tooling hoặc dependency dùng chung. | Không lặp dependency mâu thuẫn với `ingestion/requirements.txt`; xác định rõ dependency runtime và dev. | Cài đặt sạch trên virtual environment mới thành công. |
| 0.8 | Viết smoke test kiểm tra MinIO health, bucket, quyền put/get/delete object test, Trino health và catalog. | Object test phải dùng prefix riêng và dọn đúng object test, không xóa cả bucket. | `smoke_test_local.sh` trả exit code 0 khi toàn bộ local stack hoạt động. |

### Phase 1 — Crawler ghi thẳng Bronze vào MinIO

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 1.1 | Chuẩn hóa package `ingestion`, `crawler`, `storage` và các file `__init__.py`. | Tên module phải thống nhất chữ thường; tránh file `_init_.py` hoặc tên khác chuẩn Python. | Import package từ project root không lỗi. |
| 1.2 | Định nghĩa model/data contract cho job và Bronze envelope: `schema_version`, `source`, `run_id`, `ingested_at`, `source_page`, `source_url`, `job_id`, `payload`. | Bronze giữ dữ liệu gần nguồn nhất; không áp business rule như chuẩn hóa salary hoặc loại địa điểm tại crawler. | ITviec và TopCV trả về cùng envelope nhưng giữ payload riêng của nguồn. |
| 1.3 | Tách crawler ITviec thành client: đăng nhập từ environment, gọi request bằng cookie jar Playwright, lấy tổng trang từ response, parse `jobs_html` bằng BeautifulSoup và retry 429. | Không chép cookie/CSRF từ cURL. Tôn trọng rate limit; không suy diễn filter từ UTM. Hàm thu thập trả dictionary/list, không tự ghi CSV. | Client lấy được trang 1 đến trang cuối và có test bằng response fixture. |
| 1.4 | Tách crawler TopCV thành client có interface tương đương ITviec. | Selector và response mỗi nguồn khác nhau nhưng output envelope phải nhất quán. | Chạy độc lập được TopCV và trả dữ liệu đúng contract. |
| 1.5 | Tạo abstract `BronzeWriter` với các hàm như `write_batch`, `exists` và `healthcheck`. | Dùng dependency inversion: crawler không import MinIO SDK hay GCS SDK trực tiếp. | Có fake/in-memory writer để unit test ingestion không cần object storage thật. |
| 1.6 | Cài đặt `MinioBronzeWriter`: serialize JSONL vào memory stream và `put_object` thẳng lên MinIO theo partition path đã quy ước. | Ghi thẳng MinIO nghĩa là không cần tạo CSV trung gian. Dùng content type phù hợp, checksum/size, retry có giới hạn và object key chứa `run_id` để không ghi đè. | Sau một lượt crawl, object Bronze xuất hiện đúng bucket/prefix và đọc lại được đủ record. |
| 1.7 | Tạo checkpoint lưu `run_id`, nguồn, page đã hoàn tất, object key và trạng thái. | Checkpoint không thay thế tính idempotent. Chỉ đánh dấu page hoàn tất sau khi upload MinIO thành công. | Khi dừng giữa chừng, chạy lại tiếp tục từ page chưa hoàn tất hoặc tạo run mới theo cấu hình. |
| 1.8 | Viết ingestion `main.py`: tạo config, writer, crawler; crawl theo source; chia batch; ghi MinIO; tổng hợp thống kê và exit code. | Thứ tự đúng là fetch → parse → validate tối thiểu → write Bronze → checkpoint. Không log password/token. | Một command tạo được Bronze object cho cả hai nguồn. |
| 1.9 | Khai báo dependency ingestion: Playwright, BeautifulSoup, MinIO SDK, config/validation và test packages cần thiết. | Sau cài package phải chạy `playwright install`; không phụ thuộc vào browser có sẵn trên máy. | Container hoặc virtual environment mới chạy crawler được. |
| 1.10 | Viết unit/integration test cho pagination, parser, retry 429, duplicate ID, serialization và MinIO writer. | Unit test dùng fixture response và fake writer; integration test dùng bucket/prefix riêng. Không gọi website thật trong test mặc định. | Test pass và chứng minh số record ghi bằng số record crawler trả về. |

### Phase 2 — Spark Bronze → Silver

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 2.1 | Tạo `SparkSession` dùng chung, nhận cấu hình theo environment. | Không hard-code endpoint hay credential trong source. Đảm bảo version `hadoop-aws` và AWS SDK tương thích với Spark/Hadoop. | Spark session chạy được cả local và test. |
| 2.2 | Cấu hình object storage: local dùng `s3a://`, MinIO endpoint, region, credential provider và path-style; prod dùng connector GCS. | MinIO là third-party S3 store nên endpoint và path-style thường phải khai báo rõ. Credential phải truyền qua secret/env, không in trong Spark config log. | Spark đọc được Bronze trên MinIO và ghi thử Parquet trở lại MinIO. |
| 2.3 | Định nghĩa schema Bronze và Silver, quy tắc nullable, kiểu timestamp, salary, arrays và metadata. | Không dựa hoàn toàn vào schema inference; phải xử lý schema evolution bằng `schema_version`. | Record lỗi schema được cô lập, record hợp lệ có kiểu dữ liệu xác định. |
| 2.4 | Viết job Bronze → Silver: đọc partition mới, flatten payload, chuẩn hóa text/location/salary, loại trùng và ghi Parquet hoặc Iceberg. | Dedupe nên dùng khóa nguồn + `job_id` và tiêu chí bản ghi mới nhất. Không sửa Bronze. Thiết kế job idempotent khi chạy lại cùng partition. | Chạy lại cùng input không làm tăng bản ghi Silver sai. |
| 2.5 | Test transformation bằng dataset nhỏ gồm null, duplicate, salary nhiều định dạng, multi-location và schema version khác nhau. | So sánh cả schema lẫn dữ liệu; test partition output. | Test chứng minh rule chuẩn hóa và dedupe đúng. |

### Phase 3 — dbt Silver → Gold

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 3.1 | Khởi tạo dbt project, model paths, target và convention đặt tên. | dbt chịu trách nhiệm SQL transformation/test/documentation, không thay Spark cho parsing dữ liệu thô. | `dbt debug` và `dbt parse` thành công. |
| 3.2 | Tạo `profiles.yml.example` kết nối Trino; credential thật lấy từ environment. | Không commit `profiles.yml` thật. Tách target dev/prod. | Người mới cấu hình theo example và kết nối được Trino. |
| 3.3 | Khai báo Silver tables trong `_sources.yml`, freshness và source tests. | Xác định rõ catalog, schema, timezone và độ trễ dữ liệu chấp nhận được. | `dbt source freshness` và source tests chạy được. |
| 3.4 | Tạo `stg_jobs.sql` đổi tên cột, chuẩn hóa giá trị cuối và tạo khóa ổn định cho downstream. | Staging nên mỏng, tránh metric business phức tạp. | Model staging có một row logic cho mỗi job version cần dùng. |
| 3.5 | Tạo `gold_salary_metrics.sql` theo ngày, thành phố, level, role và source. | Không trộn VND/USD khi chưa quy đổi; phải lưu currency và thời điểm/tỷ giá nếu có chuyển đổi. | Metric salary có grain và đơn vị rõ ràng. |
| 3.6 | Tạo `gold_skill_trends.sql`, explode skills và tính demand theo thời gian. | Chuẩn hóa alias kỹ năng như `JS`/`JavaScript` bằng mapping có kiểm soát. | Query trả trend kỹ năng theo period/source/location. |
| 3.7 | Viết `_marts.yml` gồm description, grain, owner và generic tests. | Gold tables phải có tài liệu đủ để dashboard/bot sử dụng không cần đọc code SQL. | `dbt docs generate` hiển thị đầy đủ models và columns. |
| 3.8 | Viết singular tests như salary dương, min ≤ max và tỷ lệ null không vượt ngưỡng. | Test business rule phải loại trừ có chủ đích các record “negotiable”, không xóa im lặng. | Test phát hiện được fixture dữ liệu sai. |
| 3.9 | Tạo macros cho date grain, currency, skill normalization hoặc surrogate key dùng lặp lại. | Chỉ tạo macro khi logic thực sự lặp; tránh abstraction quá sớm. | Model dùng chung logic mà không copy SQL. |
| 3.10 | Chạy pipeline dbt đầy đủ `seed/run/test/docs` trên local stack và lưu kết quả kiểm thử. | Đảm bảo query engine đọc cùng Iceberg catalog/object storage mà Spark đã ghi. | Toàn bộ dbt pipeline pass từ Silver fixture đến Gold. |

### Phase 4 — Trino query và Discord

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 4.1 | Viết query `daily_market_trend.sql` đọc Gold theo ngày và giới hạn dữ liệu cần gửi. | Query phục vụ phải ổn định về schema, timezone và thứ tự kết quả. | Query trả đúng dataset cho report ngày. |
| 4.2 | Viết `hot_job_alert.sql` với tiêu chí hot job có thể cấu hình. | Phân biệt “HOT” do nguồn gắn nhãn với “hot” do metric nội bộ tính. | Có test case chứng minh tiêu chí chọn job. |
| 4.3 | Viết formatter chia message theo giới hạn Discord, escape ký tự và format link/salary. | Không để một record lỗi làm hỏng toàn bộ report; giới hạn độ dài message. | Formatter tạo output ổn định từ fixture. |
| 4.4 | Viết Discord bot/webhook sender với retry, timeout và dry-run. | Token nằm trong secret; chống gửi trùng bằng `run_id` hoặc alert key. | Gửi thành công vào channel test và dry-run không gửi thật. |
| 4.5 | Test formatter, empty result, Unicode, message dài, retry và duplicate alert. | Mock HTTP trong unit test; chỉ integration test mới dùng webhook thật. | Test pass mà không gửi message ngoài ý muốn. |

### Phase 5 — Airflow orchestration

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 5.1 | Tạo DAG theo chuỗi `crawl → bronze → silver → dbt → query → notify`, có schedule, retries, timeout và SLA phù hợp. | Task phải idempotent; truyền path/run_id qua metadata nhỏ, không đẩy toàn bộ job data vào XCom. | DAG chạy end-to-end và rerun một task không tạo dữ liệu trùng. |
| 5.2 | Tạo operator/hook cho Trino và Discord hoặc dùng provider chuẩn nếu phù hợp. | Connection/secret quản lý bằng Airflow Connections/secret backend. | Operator xử lý đúng success, retry và failure. |
| 5.3 | Test DAG import, dependency, task configuration và một lượt chạy local. | DAG test không được phụ thuộc website thật; mock external services khi cần. | Airflow không có import error và graph đúng thứ tự. |

### Phase 6 — Deploy GCP và thay MinIO bằng GCS

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 6.1 | Terraform module tạo GCS bucket, versioning/lifecycle và cấu trúc logical prefix tương đương MinIO. | Bucket/prefix không phải folder thật. Chọn region và retention phù hợp chi phí. | Terraform tạo bucket đúng policy và không public. |
| 6.2 | Tạo service accounts và IAM tối thiểu cho crawler, Spark, Trino và deploy. | Áp dụng least privilege; tách quyền đọc Bronze và ghi Silver/Gold nếu có thể. | Mỗi workload chỉ truy cập được phần cần thiết. |
| 6.3 | Tạo Secret Manager resources cho website credentials, Discord token và service config nhạy cảm. | Không đưa secret value vào Terraform state nếu workflow có thể tránh; kiểm soát quyền đọc secret. | Workload đọc được secret qua identity, không cần file key trong repo. |
| 6.4 | Terraform module đóng gói/deploy Cloud Function hoặc Cloud Run job cho ingestion phù hợp thời gian chạy. | Crawler Playwright và crawl nhiều trang có thể vượt giới hạn function; đánh giá Cloud Run Job nếu cần browser/runtime dài. | Runtime production chạy được browser và ghi Bronze. |
| 6.5 | Provision compute cho Spark/Trino/Airflow theo thiết kế đã chọn. | Tách control plane và data; cân nhắc managed service so với VM về vận hành/chi phí. | Workload có network và identity truy cập GCS/catalog. |
| 6.6 | Ghép modules trong `terraform/main.tf` và khai báo dependency rõ ràng. | Không tạo dependency vòng; dùng output thay vì hard-code resource name. | `terraform plan` phản ánh đúng toàn bộ hạ tầng. |
| 6.7 | Khai báo variables có type, validation, description và giá trị mặc định an toàn. | Secret không đặt làm default. Phân biệt biến theo environment. | Input sai bị chặn ngay ở validate/plan. |
| 6.8 | Khai báo outputs cần thiết như bucket, service account và endpoint; đánh dấu output nhạy cảm. | Output Terraform vẫn có thể nằm trong state/log. Không output secret nếu không cần. | Deploy script lấy được thông tin cần thiết từ output. |
| 6.9 | Tạo `prod.yaml`, dùng cùng logical path và schema với dev nhưng chọn backend GCS. | Code transformation không nên biết đang chạy MinIO hay GCS ngoài lớp config/storage. | Đổi environment không cần sửa crawler business logic. |
| 6.10 | Cài đặt `GCSBronzeWriter` theo cùng interface và contract của MinIO writer. | Hai writer phải có cùng quy tắc object key, metadata, idempotency và checksum. | Contract test chạy pass cho cả MinIO và GCS writer. |
| 6.11 | Viết tài liệu setup secret, rotation và incident response cơ bản. | CURL chứa cookie/auth token cũng là secret; phải thu hồi/rotate khi bị lộ. | Người vận hành biết tạo, cấp quyền và xoay secret. |
| 6.12 | Viết deploy scripts cho ingestion runtime và Airflow DAG, có validate/dry-run. | Script không được ghi secret ra stdout; deploy phải versioned và rollback được. | Deploy một phiên bản và rollback phiên bản trước thành công. |
| 6.13 | Chạy smoke/integration test production: crawl ít trang, ghi GCS, Spark đọc, Trino query và Discord dry-run. | Không dùng full crawl cho smoke test; gắn `run_id` test và lifecycle cleanup. | Chứng minh toàn bộ pipeline production hoạt động trước khi bật schedule. |

### Phase 7 — Monitoring và tài liệu

| Task | Nội dung cần thực hiện | Kiến thức và lưu ý | Hoàn thành khi |
|---|---|---|---|
| 7.1 | Tạo metric/alert cho crawl failure, HTTP 429, record count bất thường, object upload failure, Spark/dbt failure và DAG SLA miss. | Alert theo triệu chứng có hành động xử lý; tránh alert mọi exception đơn lẻ. | Có dashboard và test alert đến channel kiểm thử. |
| 7.2 | Cập nhật tài liệu kiến trúc, data flow, local MinIO và production GCS. | Tài liệu phải khớp code/config thật; ghi rõ quyết định kiến trúc và trade-off. | Người mới hiểu được dữ liệu đi từ source đến Gold. |
| 7.3 | Viết data dictionary cho Bronze/Silver/Gold, grain, type, nullable, owner và ví dụ. | Phân biệt source field với derived field và quy tắc PII nếu có. | Mỗi cột phục vụ phân tích đều có định nghĩa. |
| 7.4 | Hoàn thiện README: setup, run, test, troubleshooting, architecture link và security notes. | Không đưa credential mẫu có thể dùng thật. Các command phải được kiểm thử trên máy sạch. | Người mới chạy được local pipeline theo README. |

## 7. Kiến thức quan trọng cần nhớ khi thực hiện

### Bronze và MinIO

- Bronze là lớp dữ liệu logic; MinIO là nơi lưu vật lý ở local.
- Ghi thẳng MinIO bằng SDK/S3 API, không cần tạo CSV trung gian.
- Bronze append-only và có `run_id`; không update object cũ.
- Bucket không phải database và prefix không phải folder vật lý.
- Upload thành công trước rồi mới cập nhật checkpoint.
- CSV có thể giữ làm công cụ debug nhưng không được dùng làm hợp đồng giữa các
  stage.

### Spark đọc MinIO

- Spark/Hadoop dùng URI `s3a://bucket/key`.
- Cần `hadoop-aws` và AWS SDK đúng version với Hadoop đang dùng.
- Cần endpoint, region và path-style phù hợp với MinIO.
- Không hard-code access key/secret key trong source code hoặc Spark event log.

### Trino/Iceberg đọc MinIO

- Trino đọc object storage trực tiếp nhưng Iceberg vẫn cần metadata catalog.
- Với Trino mới, dùng native S3 filesystem (`fs.s3.enabled=true` và `s3.*`).
- Không dùng lại các property legacy `hive.s3.*` từ tutorial cũ.
- MinIO chứa data/metadata file; catalog quản lý thông tin table và location.

### Chuyển từ local sang production

- Giữ nguyên data contract và logical prefix giữa MinIO và GCS.
- Chỉ thay implementation của `BronzeWriter` và cấu hình Spark/Trino storage.
- Local dùng static credential trong `.env`; production ưu tiên workload identity
  hoặc service account, không dùng key file lâu dài.
- Luôn chạy contract test cho cả hai backend trước khi deploy.

## 8. Definition of Done toàn pipeline

Pipeline được coi là hoàn thành khi:

1. Crawler đăng nhập và lấy đủ pagination mà không hard-code cookie/CSRF.
2. Dữ liệu được ghi thẳng vào MinIO Bronze theo source/date/run, không cần CSV.
3. Chạy lại cùng input không tạo duplicate sai ở Silver.
4. Spark đọc được MinIO và ghi Silver thành công.
5. Trino/dbt tạo và test được các bảng Gold.
6. Airflow chạy end-to-end, retry an toàn và không truyền data lớn qua XCom.
7. Production đổi sang GCS mà không sửa logic crawler.
8. Secret không xuất hiện trong Git, log, Terraform output hoặc file dữ liệu.
9. Monitoring phát hiện được pipeline failure và data-quality anomaly.
10. README cho phép một người mới dựng và chạy local stack từ đầu.

## 9. Tài liệu kỹ thuật tham khảo

- [Trino object storage](https://trino.io/docs/current/object-storage.html)
- [Trino native S3/MinIO filesystem](https://trino.io/docs/current/object-storage/file-system-s3.html)
- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Apache Hadoop S3A connector](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/connecting.html)
