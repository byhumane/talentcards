import json
import os
from datetime import datetime
from typing import Dict, List

import requests
from google.cloud import secretmanager, storage


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


def get_reports_data(group, user) -> List:
    """Get users data from TalentCards API.

    Returns:
      Dictionary with users data.
    """
    base_url = "https://www.talentcards.io/api/v1"
    access_token = os.getenv("TALENTCARD_ACCESS_TOKEN")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    report = requests.get(
        f"{base_url}/company/groups/{group}/users/{user}/reports", headers=headers
    ).json()
    return report


def upload_json_to_gcs(
    storage_client: storage.Client,
    bucket_name: str,
    destination_blob_name: str,
    json_data: Dict,
):
    """Upload json data on Google Cloud Storage as json file.

    Args:
      storage_client (str): Storage Client.
      bucket_name (str): Bucket name to be uploaded.
      destination_blob_name (str): Destination filename.
      json_data: Data in json format.
    """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(json.dumps(json_data))


def start(request):
    date_str = datetime.now().strftime("%Y-%m-%d")
    landing_zone_bucket_name = os.getenv("HUMANE_LANDING_ZONE_BUCKET")
    storage_client = storage.Client()
    groups_path = format_folder_path("talentcard/Groups", date_str, "groups")
    groups_data = read_json_from_gcs(landing_zone_bucket_name, groups_path, storage_client)
    users_path = format_folder_path("talentcard/Users", date_str, "users")
    users_data = read_json_from_gcs(landing_zone_bucket_name, users_path, storage_client)
    groups_ids = [group_id["id"] for group_id in groups_data["data"]]
    users_ids = [user_id["id"] for user_id in users_data["data"]]
    for group in groups_ids:
        for user in users_ids:
            reports_data = get_reports_data(group, user)
            destination_blob_name = format_folder_path(
                "talentcard/Reports", date_str, f"report-{group}-{user}"
            )
            upload_json_to_gcs(
                storage_client,
                landing_zone_bucket_name,
                destination_blob_name,
                reports_data,
            )
    return "Function talentcard-reports-to-landing-zone finished successfully!"
