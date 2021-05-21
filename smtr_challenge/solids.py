__all__ = [
    "fetch_json_data",
    "generate_dataframe",
    "save_dataframe_to_csv"
]

import requests
import pandas as pd

import dagster
from dagster import solid
from dagster.core.types.dagster_type import Nothing


@solid(config_schema={"api_endpoint": str, "api_path": str})
def fetch_json_data(context: dagster.SolidExecutionContext):
    """ Fetches data from a JSON API

        Configuration schema:
        - api_endpoint (str): the API base endpoint (e.g. https://api.github.com/)
        - api_path (str): the path of the API from which you want to fetch (e.g. /users/gabriel-milan)
    """

    # Joins URL parts into a single URL, stripping unnecessary slashes
    url_parts: list = [context.solid_config["api_endpoint"],
                       context.solid_config["api_path"]]
    url: str = "/".join(s.strip("/") for s in url_parts)

    # Performs GET request to the URL
    r: requests.Response = requests.get(url)

    # Checks whether the response is OK
    if (r.ok):
        # Returns JSON parsed response
        return r.json()

    # If response is not OK, build a message and both log it
    # and raise it as an exception
    msg: str = f"Failed to fetch URL {url}: status_code {r.status_code}, message \"{r.text}\""
    context.log.error(msg)
    raise Exception(msg)


@solid(config_schema={"data_key": str})
def generate_dataframe(context: dagster.SolidExecutionContext, data: dict):
    """ Builds a Pandas dataframe from data dictionary, using "data_key"
        for extracting a list of items

        Configuration schema:
        - data_key (str): the key of the dictionary which contains the data
        (e.g on "{"asd": [{"a": 1}, {"b": 2}]}", "asd" is the data_key)
    """

    # Parses key from solid configuration
    key: str = context.solid_config["data_key"]

    # Extracts list of items
    extracted_data: list = data[key]

    # Builds dataframe and returns
    df = pd.DataFrame(extracted_data)
    return df


@solid(config_schema={"output_filename": str})
def save_dataframe_to_csv(context: dagster.SolidExecutionContext, df: pd.DataFrame) -> Nothing:
    """ Gets Pandas dataframe as input and saves it as a CSV file

        Configuration schema:
        - output_filename (str): name of the output file. If no
        ".csv" is provided at the end, it will be automatically
        added.
    """

    # Parses output filename from solid configuration
    fname: str = context.solid_config["output_filename"]

    # Adds .csv on the end if needed
    if not fname.endswith(".csv"):
        fname += ".csv"

    # Saves file
    df.to_csv(fname, index=False)
