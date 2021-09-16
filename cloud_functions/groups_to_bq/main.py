import json
import os
from datetime import datetime
from typing import Dict, List

import pandas as pd
from google.cloud import storage


def format_folder_path(table_path: str, date: str, file_name: str) -> str:
    """Formats the folder path, adding the year and month and returns the formatted folder path.

    Args:
      table_path (str): Table path.
      date (str): Date with year, month and day values to be extracted and included in the folder path.
      file_name (str): File name to be saved.

    Returns:
      The formatted folder path, with year, month and day included.
    """
    dt = datetime.strptime(date, "%Y-%m-%d")
    return f"{table_path}/year={dt.year}/month={dt.month}/day={dt.day}/{file_name}.json"


def read_json_from_gcs(
    bucket_name: str, filename: str, storage_client: storage.Client
) -> Dict:
    """Read a json file from Google Cloud Storage.

    Args:
      bucket_name (str): Bucket name with json files.
      filename (str): Json file name.
      storage_client (str): Storage Client..

    Returns:
      Dictionary with json content.
    """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.get_blob(filename)
    return json.loads(blob.download_as_bytes().decode("utf-8"))


def process_groups_data(groups_raw_data: Dict) -> pd.DataFrame:
    """Process raw groups data to get only desired data.

    Args:
      groups_raw_data (Dict): Semi-structured raw data.

    Returns:
      Pandas dataframe pandas with structured data.
    """
    groups_list = []
    for raw_data in groups_raw_data:
        for group in raw_data["data"]:
            groups_dict = {"group_id": int(group["id"])}
            attributes = group["attributes"]
            del attributes["settings"]
            groups_dict.update(attributes)
            del groups_dict["access-token"]
            groups_list.append(groups_dict)
    groups_df = pd.DataFrame(groups_list)
    return fix_columns_to_upload_to_bq(groups_df)


def fix_columns_to_upload_to_bq(df: pd.DataFrame) -> pd.DataFrame:
    """Removes `-` from dataframe columns and replace for `_`.

    Args:
      df: Dataframe to be formatted.

    Returns:
      Pandas dataframe with columns changed.
    """
    fixed_columns = [column.replace("-", "_") for column in df.columns.tolist()]
    df.columns = fixed_columns
    return df


def start(request):
    date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    landing_zone_bucket_name = os.getenv(
        "HUMANE_LANDING_ZONE_BUCKET", "humane-landing-zone"
    )
    project_id = os.getenv("PROJECT_ID", "analytics-dev-308300")
    storage_client = storage.Client()
    groups_raw_data = read_json_from_gcs(
        landing_zone_bucket_name,
        format_folder_path("talentcard/Groups", date_str, "groups"),
        storage_client,
    )
    groups_processed_df = process_groups_data(groups_raw_data)
    talentcards_dataset = "talentcards"
    users_table_name = "groups"
    groups_processed_df.to_gbq(
        f"{talentcards_dataset}.{users_table_name}",
        if_exists="replace",
        progress_bar=True,
    )
    return "Function talentcard-groups-to-bq finished successfully!"
