# llmops-weather-report

This project collects weather data from the **OpenWeather API** and
fine-tunes a lightweight language model to automatically generate
human-readable daily weather reports. It integrates data ingestion,
preprocessing, model training, experiment tracking, and API serving into
a single pipeline.

------------------------------------------------------------------------

## Features

-   **Data Ingestion**: Periodic collection of current and forecast
    weather data via OpenWeather API.
-   **Data Processing**: Converts structured weather data into
    training-ready text format.
-   **Fine-Tuning**: Uses a small seq2seq (e.g., `t5-small`) or decoder
    (e.g., `distilgpt2`) model.
-   **Experiment Tracking**: MLflow for parameters, metrics, and
    artifact management.
-   **Serving**: FastAPI endpoints for real-time weather report
    generation.
-   **Scheduling**: Apache Airflow DAGs for automated workflows.

------------------------------------------------------------------------

## Architecture

``` mermaid
flowchart LR
  A[Airflow Scheduler] --> B["Ingest: OpenWeather API"]
  B --> C["Raw Storage (data/raw)"]
  C --> D["Preprocess -> train/val/test (data/processed)"]
  D --> E["Fine-tune (Transformers)"]
  E --> F["MLflow Tracking/Artifacts"]
  F --> G["Model Registry (best.ckpt)"]
  G --> H["FastAPI Service"]
  H --> I["GET /report?city=...&lang=..."]

```

------------------------------------------------------------------------

## Tech Stack

-   **Frameworks**: Python 3.11, PyTorch, Hugging Face Transformers,
    FastAPI, Uvicorn
-   **Orchestration**: Apache Airflow
-   **Tracking**: MLflow (local file/SQLite backend)
-   **Data Source**: OpenWeather API (Current Weather + Forecast)
-   **Containerization (optional)**: Docker / docker compose

------------------------------------------------------------------------

## Folder Structure

    llmops-weather-report/
    ├─ airflow/
    │  ├─ dags/
    │  │  └─ weather_pipeline_dag.py
    ├─ app/
    │  ├─ main.py
    │  ├─ loader.py
    │  └─ schemas.py
    ├─ data/
    │  ├─ raw/
    │  └─ processed/
    ├─ ml/
    │  ├─ preprocess.py
    │  ├─ train.py
    │  ├─ infer.py
    │  └─ eval.py
    ├─ scripts/
    │  ├─ ingest_openweather.py
    │  └─ build_dataset.py
    ├─ configs/
    │  ├─ cities.yaml
    │  └─ train.yaml
    ├─ models/
    ├─ .env.example
    ├─ requirements.txt
    ├─ compose.yml
    └─ README.md

------------------------------------------------------------------------

## Quick Start

### 1. Environment Variables

``` bash
cp .env.example .env
```

Example `.env`:

    OPENWEATHER_API_KEY=your_openweather_key
    TZ=Asia/Seoul

    MLFLOW_TRACKING_URI=file:./mlruns
    MLFLOW_EXPERIMENT_NAME=weather-report

    BASE_MODEL=t5-small
    SEED=42

    SERVICE_MODEL_PATH=./models/best
    SERVICE_DEVICE=cpu

### 2. Install Dependencies

``` bash
python -m venv venv && source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Data Ingestion

``` bash
python scripts/ingest_openweather.py --city Seoul --lang en --units metric
```

### 4. Dataset Preparation

``` bash
python scripts/build_dataset.py --input_dir data/raw --output_dir data/processed
```

### 5. Fine-Tuning

``` bash
python ml/train.py --config configs/train.yaml
```

### 6. Serving

``` bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## FastAPI Endpoints

-   `GET /health` --- health check\
-   `GET /report?city=Seoul&lang=en` --- generate a weather report\
-   `POST /train` --- optional endpoint to trigger training

------------------------------------------------------------------------

## Requirements

    fastapi
    uvicorn
    python-dotenv
    pydantic
    pyyaml

    torch
    transformers
    datasets
    accelerate

    mlflow
    rouge-score
    nltk

    apache-airflow
    requests

------------------------------------------------------------------------
