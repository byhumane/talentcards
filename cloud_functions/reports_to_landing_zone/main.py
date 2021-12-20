import json
import os
from datetime import datetime

import requests
from google.cloud import storage

from time import sleep
from threading import Event, Thread
from queue import Queue

# os.environ['TALENTCARD_ACCESS_TOKEN'] = open('../../keys/talentcards.txt', 'r').readline()
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../../keys/gcp_key.json'

base_url = "https://www.talentcards.io/api/v1"
access_token = os.getenv("TALENTCARD_ACCESS_TOKEN")
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-type": "application/json",
    "Accept": "application/json",
}
event = Event()
fila = Queue()


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


def get_reports_data(group, user) -> list:
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


def get_accouts():
    """Create a dictionary with pair 'group_id' and 'user_id'"""
    groups_ids = get_groups_ids()
    for group in groups_ids:
        users_ids = get_users_id(int(group))
        for user in users_ids:
            fila.put({'group_id':int(group), 'user_id':user})
    event.set()
    fila.put('Kill')


def upload_by_account(account):
    date_str = datetime.now().strftime("%Y-%m-%d")
    landing_zone_bucket_name = 'humane-landing-zone'
    storage_client = storage.Client()
    group = account['group_id']
    user = account['user_id']
    while True:
        try:
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
        except:
            sleep(30)


def get_pool(n_th:int):
    """Return n_th Threads."""
    return [Worker(target=upload_by_account, queue=fila, name=f'Worker{n}') for n in range(n_th)]



class Worker(Thread):
    def __init__(self, target, queue, *, name='Worker'):
        super().__init__()
        self.name = name
        self.queue = queue
        self._target = target
        self._stoped = False
        print(self.name, 'started')

    def run(self):
        event.wait()
        while not self.queue.empty():
            account = self.queue.get()
            if account == 'Kill':
                self.queue.put(account)
                self._stoped = True
                break

            print(f'{self.name} group_id={account["group_id"]} user_id={account["user_id"]}')
            self._target(account)

    def join(self):
        while not self._stoped:
            sleep(.1)

#--------------------------------------------------------------------------------------------------------------------------#

def start(request=None):
    #start_time = datetime.now()
    get_accouts()
    n_th = int( (len(fila.queue))**(1/2) ) # number of threads
    thrs = get_pool(n_th)
    [th.start() for th in thrs]
    [th.join() for th in thrs]

    #time_elapsed = datetime.now() - start_time

    #print(f'Tempo total (hh:mm:ss:ms) {time_elapsed}')
