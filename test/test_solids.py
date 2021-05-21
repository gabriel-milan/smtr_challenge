import pandas as pd
from smtr_challenge import solids
from dagster import execute_solid, SolidExecutionResult


def test_fetch_json_data():
    """ Tests "fetch_json_data" solid """

    # Setup run configuration
    # Sets URL to http://webapibrt.rio.rj.gov.br/api/v1/brt
    run_config = {
        "solids": {
            "fetch_json_data": {
                "config": {
                    "api_endpoint": "http://webapibrt.rio.rj.gov.br/api/v1",
                    "api_path": "/brt"
                }
            }
        }
    }

    # Executes solid
    res = execute_solid(solids.fetch_json_data, run_config=run_config)

    # Ensures output class is as expected
    assert isinstance(res, SolidExecutionResult)

    # Ensures it has ran successfully
    assert res.success

    # Ensures output type is OK
    assert type(res.output_value()) == dict


def test_generate_dataframe():
    """ Tests "generate_dataframe" solid """

    # Setup run configuration
    # Sets data_key to "asd"
    run_config = {
        "solids": {
            "generate_dataframe": {
                "config": {
                    "data_key": "asd"
                }
            }
        }
    }

    # Build input_data
    # Arguments are named, so the argument "data"
    # will get {"asd": [{"a": 123},{"b": 456}]}
    input_data: dict = {
        "data": {
            "asd": [
                {"a": 123},
                {"b": 456}
            ]
        }
    }

    # Executes solid
    res = execute_solid(solids.generate_dataframe,
                        run_config=run_config, input_values=input_data)

    # Ensures output class is as expected
    assert isinstance(res, SolidExecutionResult)

    # Ensures it's ran successfully
    assert res.success

    # Ensures output type is OK
    assert type(res.output_value()) == pd.DataFrame

    # Ensures output matches expected dataframe
    assert res.output_value().compare(
        pd.DataFrame([{"a": 123}, {"b": 456}])).empty


def test_save_dataframe_to_csv():
    """ Tests "save_dataframe_to_csv" solid """

    # Setup run configuration
    # Sets output filename to "test.csv"
    run_config = {
        "solids": {
            "save_dataframe_to_csv": {
                "config": {
                    "output_filename": "test.csv"
                }
            }
        }
    }

    # Builds simple Pandas dataframe
    df: pd.DataFrame = pd.DataFrame([{"a": 1}, {"a": 2}])

    # Setup input data
    # Arguments are named, so the argument "df"
    # will get the dataframe previously set
    input_data: dict = {
        "df": df
    }

    # Executes the solid
    res: SolidExecutionResult = execute_solid(solids.save_dataframe_to_csv,
                                              run_config=run_config, input_values=input_data)

    # Ensures output class matches expected
    assert isinstance(res, SolidExecutionResult)

    # Ensures it's ran successfully
    assert res.success

    # Ensures there's nothing on the output
    assert res.output_value() is None

    # Ensures saved CSV file content matches
    # previously defined Pandas dataframe
    df2: pd.DataFrame = pd.read_csv("test.csv")
    assert df2.compare(df).empty
