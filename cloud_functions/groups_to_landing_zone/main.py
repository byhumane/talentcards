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


def get_groups_data() -> List:
    """Get groups data from TalentCards API.

    Returns:
      Dictionary with groups data.
    """
    base_url = "https://www.talentcards.io/api/v1"
    access_token = os.getenv("TALENTCARD_ACCESS_TOKEN")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    groups = [requests.get(f"{base_url}/company/groups", headers=headers).json()]
    num_pages = groups[0]["meta"]["last_page"]
    if num_pages > 1:
        for page in range(2, num_pages + 1):
            groups.append(
                requests.get(
                    f"{base_url}/company/groups",
                    headers=headers,
                    params={"page[number]": page},
                ).json()
            )
    return groups


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
    bucket_name = os.getenv("HUMANE_LANDING_ZONE_BUCKET")
    storage_client = storage.Client()
    destination_blob_name = format_folder_path("talentcard/Groups", date_str, "groups")
    groups_data = get_groups_data()
    upload_json_to_gcs(storage_client, bucket_name, destination_blob_name, groups_data)
    return "Function talentcard-groups-to-landing-zone finished successfully!"
