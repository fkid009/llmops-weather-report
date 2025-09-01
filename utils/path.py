import os
from pathlib import Path

# 프로젝트 루트 기준
PROJECT_ROOT = Path("/opt/project")

# 주요 폴더
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
TMP_DIR = PROJECT_ROOT / "tmp"

CONFIG_DIR = PROJECT_ROOT / "configs"
MODELS_DIR = PROJECT_ROOT / "models"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
UTILS_DIR = PROJECT_ROOT / "utils"
INIT_DATA_DIR = DATA_DIR / "init"
INIT_TMP_DIR = TMP_DIR / "init"

# 세부 파일들
CITIES_YAML = CONFIG_DIR / "cities.yaml"
TRAIN_CONFIG = CONFIG_DIR / "train.yaml"

# MLflow
MLRUNS_DIR = PROJECT_ROOT / "mlruns"