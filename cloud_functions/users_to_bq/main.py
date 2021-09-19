import json
import os
import pytz
from datetime import datetime, timedelta
from typing import Dict
import dateutil.relativedelta
import pandas as pd
from google.cloud import bigquery, storage

# environment can be local or gcp
# used to define the need for autentication
environment='gcp'
if environment=='local':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../keys/gcp_key.json'

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


def process_user_data(users_raw_data: Dict, date: datetime, group_id: int) -> pd.DataFrame:
    """Process raw users data to get only desired data.

    Args:
      users_raw_data (Dict): Semi-structured raw data.
      date (datetime): Date.
      group_id (int): Group id.


    Returns:
      Pandas dataframe pandas with structured data.
    """
    users_list = []
    for raw_data in users_raw_data:
        for user in raw_data["data"]:
            users_dict = {"user_id": user["id"], "group_id": int(group_id)}
            users_dict.update(user["attributes"])
            users_dict["extraction_timestamp"] = date
            users_list.append(users_dict)
    users_df = pd.DataFrame(users_list)
    return fix_columns_to_upload_to_bq(users_df)


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


def start(request=None):
    date = datetime.utcnow()
    date_str = date.strftime("%Y-%m-%d")
    landing_zone_bucket_name = os.getenv(
        "HUMANE_LANDING_ZONE_BUCKET", "humane-landing-zone"
    )
    project_id = os.getenv("PROJECT_ID", "analytics-dev-308300")
    storage_client = storage.Client()
    groups_path = format_folder_path("talentcard/Groups", date_str, "groups")
    print('groups_path:\n',groups_path,"\n")
    groups_data = read_json_from_gcs(landing_zone_bucket_name, groups_path, storage_client)
    print('groups_data:\n',groups_data,'\n')
    groups_ids = get_groups_ids(groups_data)
    print('groups_ids:\n',groups_ids,'\n')
    users_processed_df_list = []
    for group_id in groups_ids:
      print('group_id:\n',group_id,'\n')
      users_raw_data = read_json_from_gcs(
          landing_zone_bucket_name,
          format_folder_path("talentcard/Users", date_str, f"users-{group_id}"),
          storage_client,
        )
      print('users_raw_data:\n',users_raw_data,'\n')
      users_processed_df = process_user_data(users_raw_data, date.strftime("%Y-%m-%d %H:%M:%S"), group_id)
      display('users_processed_df:\n',users_processed_df,'\n')
      users_processed_df_list.append(users_processed_df)
      display('users_processed_df_list:\n',users_processed_df_list,'\n')
    users_processed_df_final = pd.concat(users_processed_df_list)
    # talentcards_dataset = "talentcards"
    # users_table_name = "users"
    # users_processed_df_final.to_gbq(
    #     f"{talentcards_dataset}.{users_table_name}",
    #     if_exists="append",
    #     progress_bar=True,
    # )
    return "Function talentcard-users-to-bq finished successfully!"
