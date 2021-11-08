import json
import os
from datetime import datetime
from typing import List

import requests
from google.cloud import storage

# os.environ['TALENTCARD_ACCESS_TOKEN'] = open('../../keys/talentcards.txt', 'r').readline()
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../keys/gcp_key.json'

base_url = "https://www.talentcards.io/api/v1"
access_token = os.getenv("TALENTCARD_ACCESS_TOKEN")
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-type": "application/json",
    "Accept": "application/json",
}


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
    reports = [
        requests.get(
            f"{base_url}/company/groups/{group}/users/{user}/reports", headers=headers
        ).json()
    ]
    if "errors" not in reports[0]:
        num_pages = reports[0]["meta"]["last_page"]
        if num_pages > 1:
            for page in range(2, num_pages + 1):
                reports.append(
                    requests.get(
                        f"{base_url}/company/groups/{group}/users/{user}/reports",
                        headers=headers,
                        params={"page[number]": page},
                    ).json()
                )
    return reports


def upload_json_to_gcs(
        storage_client: storage.Client,
        bucket_name: str,
        destination_blob_name: str,
        json_data,
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


def get_groups_ids():
    """Get groups id from TalentCards API.
    Returns:
      List with groups id.
    """
    cia = requests.get(f"{base_url}/company", headers=headers).json()
    groups_list = [cia['relationships']['groups']['data'][x]['id'] for x in
                   range(len(cia['relationships']['groups']['data']))]
    return groups_list


def get_users_id(group_id: int):
    """Get users id from TalentCards API.

    Returns:
      List with users id.
    """
    users = [requests.get(
        f"{base_url}/company/groups/{group_id}/users", headers=headers
    ).json()]
    num_pages = users[0]["meta"]["last_page"]
    if num_pages > 1:
        for page in range(2, num_pages + 1):
            users.append(requests.get(
                f"{base_url}/company/groups/{group_id}/users",
                headers=headers,
                params={"page[number]": page},
            ).json())
    users_list = (
        [users[page]['data'][user]['id']
         for page in range(len(users))
         for user in range(len(users[page]['data']))]
    )
    return users_list


def start(request=None):
    date_str = datetime.now().strftime("%Y-%m-%d")
    landing_zone_bucket_name = 'humane-landing-zone'
    storage_client = storage.Client()
    groups_ids = get_groups_ids()
    for group in groups_ids:
        users_ids = get_users_id(group)
        for user in users_ids:
            reports_data = get_reports_data(group, user)
            if "errors" not in reports_data[0]:
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


start()
