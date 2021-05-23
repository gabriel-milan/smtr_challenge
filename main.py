# Import pipelines so they are accessible
from smtr_challenge import pipelines

# Import `execute_pipeline` so this script
# can be executable
from dagster import execute_pipeline

# Name `flow` as the `extract_csv_from_api`
# pipeline so dagit and dagster CLI are
# aware of it.
flow = pipelines.extract_csv_from_api

# Just to ensure this script is executable
if __name__ == "__main__":
    execute_pipeline(pipelines.extract_csv_from_api)
