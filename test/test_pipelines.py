from dagster import execute_pipeline, PipelineExecutionResult
from smtr_challenge import pipelines
import pandas as pd


def test_extract_csv_from_api():
    """ Tests "extract_csv_from_api" pipeline """

    # Setup run configuration
    # Using this configuration pipeline will
    # - Fetch data from http://webapibrt.rio.rj.gov.br/api/v1/brt
    # - Extract list of items from key "veiculos" and parse it to
    # a Pandas dataframe
    # - Save a CSV file containing the data from this dataframe
    run_config = {
        "solids": {
            "fetch_json_data": {
                "config": {
                    "api_endpoint": "http://webapibrt.rio.rj.gov.br/api/v1",
                    "api_path": "/brt"
                }
            },
            "generate_dataframe": {
                "config": {
                    "data_key": "veiculos"
                }
            },
            "save_dataframe_to_csv": {
                "config": {
                    "output_filename": "veiculos.csv"
                }
            }
        }
    }

    # Executing pipeline
    res = execute_pipeline(
        pipelines.extract_csv_from_api, run_config=run_config)

    # Ensures output class is as expected
    assert isinstance(res, PipelineExecutionResult)

    # Ensures it has ran successfully
    assert res.success

    # Ensures output types for all solids are OK
    assert type(res.output_for_solid("fetch_json_data")
                ) == dict  # must be a dict
    assert type(res.output_for_solid("generate_dataframe")
                ) == pd.DataFrame  # must be a dataframe
    assert res.output_for_solid(
        "save_dataframe_to_csv") is None  # must be None

    # Ensures inputs for solid "generate_dataframe" were OK
    assert "veiculos" in res.output_for_solid(
        "fetch_json_data")  # data_key must be in it
    assert type(res.output_for_solid("fetch_json_data")[
                "veiculos"]) == list  # inside data_key must be a list
    assert len(res.output_for_solid("fetch_json_data")[
               "veiculos"]) > 0  # the list size should be > 0

    # Ensures inputs for solid "save_dataframe_to_csv" were OK
    assert res.output_for_solid("generate_dataframe").shape[0] == len(
        res.output_for_solid("fetch_json_data")["veiculos"])  # no. of rows must match list size
