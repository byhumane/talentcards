import json
import os
from datetime import datetime
from typing import Dict

import dateutil.relativedelta
import pandas as pd
from google.cloud import bigquery, storage


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


def process_sets_data(sets_raw_data: Dict, group_id: int) -> pd.DataFrame:
    """Process raw sets data to get only desired data.

    Args:
      sets_raw_data (Dict): Semi-structured raw data.
      group_id (int): Group id.

    Returns:
      Pandas dataframe pandas with structured data.
    """
    sets_list = []
    for raw_data in sets_raw_data:
        for set in raw_data["data"]:
            sets_dict = {"group_id": int(group_id), "set_id": int(set["id"])}
            attributes = set["attributes"]
            del attributes["settings"]
            sets_dict.update(attributes)
            sets_list.append(sets_dict)
    sets_df = pd.DataFrame(sets_list)
    return fix_columns_to_upload_to_bq(sets_df)


def fix_columns_to_upload_to_bq(df: pd.DataFrame):
    """Removes `-` from dataframe columns and replace for `_`.

    Args:
      df: Dataframe to be formatted.

    Returns:
      Pandas dataframe with columns changed.
    """
    fixed_columns = [column.replace("-", "_") for column in df.columns.tolist()]
    df.columns = fixed_columns
    return df


def get_groups_ids(groups_data):
    groups_ids = []
    for group_data in groups_data:
        groups_ids.extend([group_id["id"] for group_id in group_data["data"]])
    return groups_ids


def start(request):
    date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    landing_zone_bucket_name = os.getenv(
        "HUMANE_LANDING_ZONE_BUCKET", "humane-landing-zone"
    )
    project_id = os.getenv("PROJECT_ID", "analytics-dev-308300")
    storage_client = storage.Client()
    groups_path = format_folder_path("talentcard/Groups", date_str, "groups")
    groups_data = read_json_from_gcs(landing_zone_bucket_name, groups_path, storage_client)
    groups_ids = get_groups_ids(groups_data)
    sets_processed_df_list = []
    for group_id in groups_ids:
        sets_raw_data = read_json_from_gcs(
            landing_zone_bucket_name,
            format_folder_path("talentcard/Sets", date_str, f"sets-{group_id}"),
            storage_client,
        )
        sets_processed_df = process_sets_data(sets_raw_data, group_id)
        sets_processed_df_list.append(sets_processed_df)
    sets_processed_df_final = pd.concat(sets_processed_df_list)
    talentcards_dataset = "talentcards"
    sets_table_name = "sets"
    sets_processed_df_final.to_gbq(
        f"{talentcards_dataset}.{sets_table_name}",
        if_exists="replace",
        progress_bar=True,
    )
    return "Function talentcard-sets-to-bq finished successfully!"
