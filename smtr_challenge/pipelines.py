from . import solids
from dagster import pipeline


@pipeline
def extract_csv_from_api():
    """ Extracts JSON data from an API, parse list
    of items from it and then save a CSV file.

    Configuration schema:
    - fetch_json_data (solid)
        - api_endpoint (str): the API base endpoint (e.g. https://api.github.com/)
        - api_path (str): the path of the API from which you want to fetch (e.g. /users/gabriel-milan)
    - generate_dataframe (solid)
        - data_key (str): the key of the dictionary which contains the data
        (e.g on "{"asd": [{"a": 1}, {"b": 2}]}", "asd" is the data_key)
    - save_dataframe_to_csv (solid)
        - output_filename (str): name of the output file. If no
        ".csv" is provided at the end, it will be automatically
        added.

    """

    # DAG execution is:
    # 1st - fetch_json_data
    # 2nd - generate_dataframe
    # 3rd - save_dataframe_to_csv
    solids.save_dataframe_to_csv(
        solids.generate_dataframe(
            solids.fetch_json_data()))
