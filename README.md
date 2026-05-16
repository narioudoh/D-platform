# Modern Data Platform

## Overview

 This is a `battery included`Data Plaform built for high-performance, Python-native data engineering. The design intentionally moves away from heavy JVM-based tools (e.g., Spark) and favors fast, in-process Python tooling such as Polars plus cloud-scale services (Databricks) where appropriate.

Primary goals:
- Fast, memory-efficient data processing with Polars
- Python-first developer experience and minimal JVM overhead
- Reproducible workflows and orchestration with Prefect
- S3-compatible object storage via MinIO for local and cloud parity
- Light integration points for Databricks and dbt for operational workloads

Key technologies:
- Polars — in-memory DataFrame engine (Rust-backed, Python API)
- Databricks — optional cloud execution and managed compute
- Prefect — orchestration and observability for flows
- MinIO — local S3-compatible object store for testing
- dbt — transformation tooling for SQL-based models and testing
- Docker / Docker Compose — local environment for services

## What this repo contains

- `docker-compose.yml` — local services (MinIO, Prefect agent/server, etc.)
- `requirements.txt` — Python dependencies for local development
- `processes/` — example data workflows and helper scripts (Polars-friendly)
- `data/bronze/` — sample landing area for ingested files
- `plat/` — Python virtual environment used for development

This project is intentionally minimal so you can quickly iterate on data logic in Python (Polars) and then scale or productionize parts using Databricks and dbt.

## Quickstart (local)

1. Install prerequisites: Docker, Docker Compose, Python 3.10+
2. Activate the provided virtual environment (Windows PowerShell):

```powershell
& plat\\Scripts\\Activate.ps1
```

Or on Bash:

```bash
source plat/Scripts/activate
```

3. Install Python dependencies (optional if you prefer the Docker services):

```bash
pip install -r requirements.txt
```

4. Start local services with Docker Compose:

```bash
docker compose up --build
```

This brings up MinIO and any orchestration services defined in `docker-compose.yml` so you can run and test flows locally.

## Running example workflows

- Inspect and run the sample scripts in `processes/` to see Polars-based processing and MinIO interactions. Example:

```bash
python processes/main.py
```

- Use Prefect to run and observe flows when the Prefect services are running (see `docker-compose.yml`).

## Deployment
```bash
prefect deploy --name crypto-ingestion-pipeline
```

## Polars-first design notes

- Use Polars for CPU- and memory-efficient transforms, joins, and aggregations.
- Keep extraction and pipeline in pure Python where possible; add dbt models for SQL-first teams.
- Use Databricks only for large-scale, managed execution when necessary — test locally in Polars first.

## Storage and infra

- MinIO provides an S3-compatible API for local testing and parity with cloud storage.
- Secrets and cloud credentials should be injected via environment variables or a secrets manager; avoid committing credentials.

## Unified design principle
- Using databricks unified capability, cloud-based data intelligence platform designed to process, store, clean, share, and analyze large datasets

## Project structure (short)

- `docker-compose.yml` — service definitions
- `prefect.yml` - essential prefect configuration
- `requirements.txt` — Python dependencies
- `processes/` — example workflows and helpers scripts
- `data/` — sample landing and processed folders
- `dockerfile` - handles what the processes used by the `docker-compose.yml` file


- Standardize environment variables and document `.env` usage.
- Convert heavier SQL/warehouse transformations to `dbt` modules where appropriate.
- Add CI checks and small unit tests for key `processes/` functions.

----

## 📄 License

MIT License

Copyright (c) 2025  NarioU-sys

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

## 🙏 Acknowledgments

- Minio for Data Lake platform
- Databricks for Unified data engine
- Minio + Databricks  for Lakehouse  (`using the medallion Architecture`)
- Polar for data processing
- Docker for containerization
- prefect for Orchestration

## 📞 Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting section
- Review the logs for error details

---

**Happy Data Engineering! 🎉**





