import json
import os
from datetime import datetime
from typing import Dict, List

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


def process_reports_data(
    groups_ids: List[int],
    users_ids: List[int],
    date_str: datetime,
    storage_client: storage.Client,
) -> pd.DataFrame:
    """Process raw reports data to get only desired data.

    Args:
      groups_ids: Groups ids to be processed.
      users_ids: Users ids to be processed.
      date_str: Date in string format.
      storage_client: Storage Client.

    Returns:
      Pandas dataframe pandas with structured data.
    """
    reports_list = []
    for group_id in groups_ids:
        for user_id in users_ids:
            reports_data = read_json_from_gcs(
                "humane-landing-zone",
                format_folder_path(
                    "talentcard/Reports", date_str, f"report-{group_id}-{user_id}"
                ),
                storage_client,
            )
            if "errors" not in reports_data:
                for report in reports_data["data"]:
                    reports_dict = {
                        "set_id": report["id"],
                        "group_id": group_id,
                        "user_id": user_id,
                    }
                    if "report" in report["meta"]:
                        reports_dict.update(report["meta"]["report"])
                    reports_dict["date_str"] = date_str
                    reports_list.append(reports_dict)
    reports_df = pd.DataFrame(reports_list)
    return fix_columns_to_upload_to_bq(reports_df)


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


def start(request):
    date = datetime.now()
    date_str = date.strftime("%Y-%m-%d")
    landing_zone_bucket_name = os.getenv(
        "HUMANE_LANDING_ZONE_BUCKET", "humane-landing-zone"
    )
    project_id = os.getenv("PROJECT_ID", "analytics-dev-308300")
    storage_client = storage.Client()

    groups_path = format_folder_path("talentcard/Groups", date_str, "groups")
    groups_data = read_json_from_gcs(
        landing_zone_bucket_name, groups_path, storage_client
    )
    users_path = format_folder_path("talentcard/Users", date_str, "users")
    users_data = read_json_from_gcs(
        landing_zone_bucket_name, users_path, storage_client
    )
    groups_ids = [group_id["id"] for group_id in groups_data["data"]]
    users_ids = [user_id["id"] for user_id in users_data["data"]]
    reports_processed_df = process_reports_data(
        groups_ids, users_ids, date_str, storage_client
    )
    talentcards_dataset = "talentcards"
    users_table_name = "reports"
    reports_processed_df.to_gbq(
        f"{talentcards_dataset}.{users_table_name}",
        if_exists="append",
        progress_bar=True,
    )
    return "Function talentcard-reports-to-bq finished successfully!"
