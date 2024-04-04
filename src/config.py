"""
    # notebook.ipynb
    |   from projectname.config import data_path
    |   import pandas as pd
    |   df = pd.read_csv(data_path)  # clean!
By using these config.py files,
we get clean code in exchange for an investment of time naming variables logically.
"""

# %% Import libraries
from pathlib import Path
from sqlalchemy import create_engine

# %% Define project's paths
project_path = Path(__file__).parent.resolve()
project_dir = project_path.parent.resolve()

data_analyze_dir = project_dir / "data/analyze"
data_external_dir = project_dir / "data/external"
data_precleaned_dir = project_dir / "data/precleaned"
data_processed_dir = project_dir / "data/processed"
data_raw_dir = project_dir / "data/raw"

docs_dir = project_dir / "docs"
models_dir = project_dir / "models"
pipelines_dir = project_dir / "pipelines"
reports_dir = project_dir / "reports"
temp_dir = project_dir / "temp"
tensorboard_dir = project_dir / "tensorboard"

mlflow_tracking_uri = "../mlflow/mlruns"
mlflow_dir = "../mlflow"

model_path = "../mlflow/mlruns......"

# %% Some project stuff
# Data storage place
s3_data_local_path = Path("z:/S3/mg-bol/us/data")
s3_neo4j_local_path = Path("z:/S3/mg-bol/neo4j")
s3_profiles_local_path = Path("z:/S3/mg-bol-graph/us/neo4j/profiles")
s3_reports_local_path = Path("z:/S3/mg-bol/us/reports")
s3_data_aws_path = Path("")

# PostgreSQL url = "{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"
pg_connection_url = "postgres:admin@localhost:5432/mg-bol"
pg_engine_local = create_engine(f"postgresql://{pg_connection_url}")
