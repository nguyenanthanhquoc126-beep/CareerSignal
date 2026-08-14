# CareerSignal

Turning Job Data into Market Signals.

## Local lakehouse

Docker Compose runs MinIO, Spark, Nessie, and a Trino cluster. Nessie is the
metadata catalog for Iceberg tables; table data and Iceberg metadata files are
stored in the `warehouse` bucket on MinIO. `minio-init` creates the `bronze`,
`silver`, and `warehouse` buckets automatically, while Nessie's commit history
is persisted in the `nessie-data` Docker volume.

Create `.env` from the example if it does not exist, set the credentials, then
start the stack:

```bash
test -f .env || cp .env.example .env
docker compose up -d --build --wait
docker compose ps
```

Local endpoints:

- Nessie API config: <http://localhost:19120/api/v2/config>
- MinIO API: <http://localhost:9000>
- MinIO Console: <http://localhost:9001>
- Trino: <http://localhost:8085>
- Spark master UI: <http://localhost:8080>

The Trino catalog is named `nessie`. Verify it and run a small end-to-end
Iceberg smoke test:

```bash
docker compose exec -T trino-coordinator trino --execute "SHOW CATALOGS"
docker compose exec -T trino-coordinator trino --execute "DROP TABLE IF EXISTS nessie.smoke.catalog_check"
docker compose exec -T trino-coordinator trino --execute "DROP SCHEMA IF EXISTS nessie.smoke"
docker compose exec -T trino-coordinator trino --execute "CREATE SCHEMA nessie.smoke"
docker compose exec -T trino-coordinator trino --execute "CREATE TABLE nessie.smoke.catalog_check (id BIGINT, message VARCHAR)"
docker compose exec -T trino-coordinator trino --execute "INSERT INTO nessie.smoke.catalog_check VALUES (1, 'nessie works')"
docker compose exec -T trino-coordinator trino --execute "SELECT * FROM nessie.smoke.catalog_check"
docker compose exec -T trino-coordinator trino --execute "DROP TABLE nessie.smoke.catalog_check"
docker compose exec -T trino-coordinator trino --execute "DROP SCHEMA nessie.smoke"
```

The current Spark jobs still write plain Parquet to `s3a://silver/...`. Plain
Parquet files are not registered in Nessie automatically. Migrating those jobs
to named Iceberg tables is a separate change that requires the Iceberg Spark
runtime and catalog-aware writes.
