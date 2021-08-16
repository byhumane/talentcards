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


def get_sets_data(base_url: str, headers: Dict, group_id: int) -> List:
    """Get sets data from TalentCards API.

    Returns:
      List of dictionary with sets data.
    """
    sets = [requests.get(
        f"{base_url}/company/groups/{group_id}/sets", headers=headers
    ).json()]
    num_pages = sets[0]["meta"]["last_page"]
    if num_pages > 1:
        for page in range(2, num_pages + 1):
            sets.append(requests.get(
                f"{base_url}/company/groups/{group_id}/sets",
                headers=headers,
                params={"page": page},
            ).json())
    return sets


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


def get_groups_ids(base_url: str, headers: str) -> List[int]:
    """Get groups ids from TalentCards API.

    Returns:
      List with group ids.
    """
    groups_data = [requests.get(f"{base_url}/company/groups", headers=headers).json()]
    num_pages = groups_data[0]["meta"]["last_page"]
    if num_pages > 1:
        for page in range(2, num_pages + 1):
            groups_data.append(
                requests.get(
                    f"{base_url}/company/groups",
                    headers=headers,
                    params={"page": page},
                ).json()
            )
    groups_ids = []
    for group_data in groups_data:
        groups_ids.extend([group_id["id"] for group_id in group_data["data"]])
    return groups_ids


def start(request):
    date_str = datetime.now().strftime("%Y-%m-%d")
    bucket_name = os.getenv("HUMANE_LANDING_ZONE_BUCKET")
    storage_client = storage.Client()
    base_url = "https://www.talentcards.io/api/v1"
    access_token = os.getenv("TALENTCARD_ACCESS_TOKEN")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    groups_ids = get_groups_ids(base_url, headers)
    for group_id in groups_ids:
        destination_blob_name = format_folder_path("talentcard/Sets", date_str, f"sets-{group_id}")
        sets_data = get_sets_data(base_url, headers, group_id)
        upload_json_to_gcs(storage_client, bucket_name, destination_blob_name, sets_data)
    return "Function talentcard-sets-to-landing-zone finished successfully!"
