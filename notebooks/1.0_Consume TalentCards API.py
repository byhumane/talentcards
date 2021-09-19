# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.12.0
#   kernelspec:
#     display_name: 'Python 3.9.7 64-bit (''std_env'': conda)'
#     name: python3
# ---

# %% [markdown]
# # GOAL
# - Interact with TalentCards apis
# - Get users data
# - Get activity data

# %% [markdown]
# # PACKAGES

# %%
import pandas as pd
import requests
from datetime import datetime

# from google.oauth2 import service_account
# from oauth2client.service_account import ServiceAccountCredentials

# %% [markdown] tags=[]
# # PARAMETERS

# %%
access_token = open("../keys/talentcards.txt", mode="r").readline()
group = 1818
today = pd.Timestamp.today().strftime("%Y-%m-%d")
today_files = pd.Timestamp.today().strftime("%Y%m%d")

group=1818
user = 20129


# %% [markdown]
# # FUNCTIONS

# %% [markdown] tags=[]
# ## fix_columns_to_upload_to_bq

# %%
def fix_columns_to_upload_to_bq(df):
    fixed_columns = [column.replace("-", "_") for column in df.columns.tolist()]
    df.columns = fixed_columns
    return df


# %% [markdown] tags=[]
# ## get_users_data

# %%
def get_users_data(group_id=1818):
    """Get user details data from Talentlms API.

    Returns:
      Dictionary with userS details data.
    """
    users = []
    base_url = "https://www.talentcards.io/api/v1"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    users=[requests.get(f"{base_url}/company/groups/{group_id}/users", headers=headers).json()]
    num_pages = users[0]["meta"]["last_page"]
    if num_pages > 1:
        for page in range(2, num_pages + 1):
            users.append(
                requests.get(
                    f"{base_url}/company/groups/{group_id}/users",
                    headers=headers,
                    params={"page[number]": page},
                ).json()
            )
    return users


# %% [markdown]
# ## process_user_data

# %% tags=[]
def process_user_data(raw_data, date):
    """Process raw data to get only desired data.

    Args:
      date:
      raw_data (List): List of jason api responses.

    Returns:
      Pandas dataframe pandas with structured data.
    """
    users_df = pd.DataFrame()
    for response in raw_data:
        users_list = []
        for user in response["data"]:
            users_dict = {"user_id": user["id"]}
            users_dict.update(user["attributes"])
            users_dict["extraction_timestamp"] = date
            users_list.append(users_dict)
        response_df = pd.DataFrame(users_list)
        users_df = users_df.append(response_df, ignore_index=True)
    users_df = users_df.sort_values(by="user_id", ignore_index=True)
    return fix_columns_to_upload_to_bq(users_df)


# %% [markdown]
# ## get_reports_data

# %%
def get_reports_data(group=1818, user=20129):
    """Get user details data from Talentlms API.

    Returns:
      Dictionary with userS details data.
    """
    base_url = "https://www.talentcards.io/api/v1"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Accept": "application/json",
    }
    response = []
    response.append(
        requests.get(
            f"{base_url}/company/groups/{group}/users/{user}/reports",
            headers=headers,
        ).json()
    )
    try:    
        num_pages = response[0]["meta"]["last_page"]
        if num_pages > 1:
            for page in range(2, num_pages + 1):
                response.append(
                    requests.get(
                        f"{base_url}/company/groups/{group}/users/{user}/reports",
                        headers=headers,
                    ).json()
                )
        user_report = {"group": group, "user": user, "reports": response}
    except:
        pass
    return user_report


# %% [markdown]
# ## process_reports_data

# %%
def process_reports_data(user_report_dict):
    """Process json user_report to get only desired data.

    Args:
      date:
      reports_data (Dict): Semi-structured json data.

    Returns:
      Pandas dataframe pandas with structured data.
    """

    report = []
    for page in user_report_dict["reports"]:
        for entry in page["data"]:
            if entry["type"] == "user-set-reports":
                report_dict = {
                    "group_id": user_report_dict["group"],
                    "user_id": user_report_dict["user"],
                    "sequence_id": "",
                    "set_id": entry["id"],
                    "set-tests": entry["meta"]["report"]["set-tests"],
                    "finished-tests": entry["meta"]["report"]["finished-tests"],
                    "progress": entry["meta"]["report"]["progress"],
                    "cards": entry["meta"]["report"]["cards"],
                    "tests": entry["meta"]["report"]["tests"],
                    "started-at": entry["meta"]["report"]["started-at"],
                    "completed-at": entry["meta"]["report"]["completed-at"],
                }
                report.append(report_dict)
            elif entry["type"] == "user-sequence-reports":
                for card_set in entry["meta"]["reports"]:
                    report_dict = {
                        "group_id": user_report_dict["group"],
                        "user_id": user_report_dict["user"],
                        "sequence_id": entry["id"],
                        "set_id": card_set["id"],
                        "set-tests": card_set["meta"]["report"]["set-tests"],
                        "finished-tests": card_set["meta"]["report"]["finished-tests"],
                        "progress": card_set["meta"]["report"]["progress"],
                        "cards": card_set["meta"]["report"]["cards"],
                        "tests": card_set["meta"]["report"]["tests"],
                        "started-at": card_set["meta"]["report"]["started-at"],
                        "completed-at": card_set["meta"]["report"]["completed-at"],
                    }
                    report.append(report_dict)
        reports_df = pd.DataFrame(report)
        reports_df["started-at"] = pd.to_datetime(reports_df["started-at"].fillna(pd.NaT),utc=True).dt.tz_convert("America/Sao_paulo")
        reports_df["completed-at"] = pd.to_datetime(reports_df["completed-at"].fillna(pd.NaT),utc=True).dt.tz_convert("America/Sao_paulo")

    return fix_columns_to_upload_to_bq(reports_df)


# %% [markdown] tags=[]
# # DATA WRANGLING

# %% [markdown]
# ##  users data

# %%
df_users = process_user_data(get_users_data(), today)

# %%
df_users.sample(5)

# %%
df_users.dtypes

# %%
df_users.to_excel(
    f"../data/out/{today_files}_TalenCards users extraction.xlsx", index=False
)

# %%
df_users.shape

# %% [markdown]
# ## user report

# %%
user_report = get_reports_data(group=group, user=user)

# %%
df_report = process_reports_data(user_report_dict=user_report)
df_report

# %% [markdown]
# ## all users reports

# %%
users_id = list(df_users["user_id"].unique())

df_reports = pd.DataFrame()
for user in users_id:
    df_reports = df_reports.append(
        process_reports_data(get_reports_data(group=group, user=user)),
        ignore_index=True,
    )


# %%
df_reports=df_reports.sort_values(by=["completed_at", "user_id"],ascending=False, ignore_index=True)
df_reports['started_at']=df_reports['started_at'].astype('str')
df_reports['completed_at']=df_reports['completed_at'].astype('str')
df_reports.to_excel(
    f"../data/out/{today_files}_TalentCards users reports.xlsx", index=False
)

# %% [markdown] tags=[]
# # SCRIPTING
