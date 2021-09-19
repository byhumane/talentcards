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
#     display_name: Python [conda env:std_env]
#     language: python
#     name: conda-env-std_env-py
# ---

# %% [markdown]
# # GOAL
# - Write here the notebook onjectives.

# %% [markdown]
# # PACKAGES

# %%
import pandas as pd
import pandas_gbq
import gcsfs
from typing import Dict, List

# %% tags=["active-ipynb"]
# # for use with notebooks
# import os

# %% [markdown]
# # PARAMETERS

# %% tags=[]
project_id = "analytics-dev-308300"
file_path = 'gs://humane-landing-zone/manual'
file_name='talentcards_user_list.xlsx'
users_table='talentcards.users_manual'


# %% tags=["active-ipynb"]
# # for use with notebooks
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.getcwd().replace('notebooks','keys/gcp_key.json')
# pd.set_option("display.max_rows", 200)
# pd.set_option("display.max_columns", None)
# pd.set_option("display.width", None)
# pd.set_option("display.max_colwidth", None)

# %% [markdown]
# # FUNCTIONS

# %% [markdown]
# ## Format folder path with date structure

# %%
def format_folder_path(table_path: str, date: str, file_name: str) -> str:
    """Formats the folder path, adding the year and month and returns the formatted folder path.

    Args:
      table_path (str): Table path.
      date (str): Date with year, month and day values to be extracted and included in the folder path.
      file_name (str): File name to be saved.

    Returns:
      The formatted folder path, with year, month and day included.
    """
    dt = pd.to_datetime(date)
    return f"{table_path}/year={dt.year}/month={dt.month}/day={dt.day}/{file_name}"


# %% [markdown]
# ## Get excel file from GCS with users_data

# %%
def get_users_xlsx(project_id, file_path, file_name):
    """"
    (str,str,str)-->df
    """
    fs=gcsfs.GCSFileSystem(project=project_id,access='read_write')
    with fs.open(file_path+'/'+file_name) as users_file:
        users_df = pd.read_excel(users_file)
    users_df['Identifier']=users_df['Identifier'].replace('-','',regex=True)
    fs.cp(
        file_path+'/'+file_name,
        format_folder_path(
            table_path='gs://humane-landing-zone/manual/talentcards_users',
            date=pd.Timestamp.today().strftime("%Y-%m-%d"),
            file_name=file_name
        ),on_error='replace'
    )
    return users_df


# %% [markdown]
# ## calculate timedelta_since_lst_login

# %%
def transform2timedelta(string_like_timedelta):    
    """
    (str)-->pd.Timedelta
    
    Exemples:
    >>transform2timedelta('2 days ago')
    Timedelta('2 days 00:00:00')

    >>transform2timedelta('2 weeks ago')
    Timedelta('14 days 00:00:00')

    >>transform2timedelta('1 week ago')
    Timedelta('7 days 00:00:00')

    >>transform2timedelta('1 month ago')
    Timedelta('30 days 00:00:00')
    
    """
    if string_like_timedelta == 'Never':
        new_time_delta=pd.NaT
    elif 'ago' in string_like_timedelta:
        new_time_delta=string_like_timedelta.replace(' ago','')
        if 'week' in new_time_delta:
            new_time_delta=new_time_delta.replace('weeks','W').replace('week','W')
        elif 'month' in new_time_delta:
            new_time_delta=(new_time_delta
                            .replace('months','D')
                            .replace('month','D')
                            .replace(new_time_delta.split()[0],str(int(new_time_delta.split()[0])*30))
                           )
    return pd.Timedelta(new_time_delta).days


# %% tags=["active-ipynb"]
# # assertions (notebook only)
#
# #assert type(transform2timedelta('Never'))==None
#
# assert transform2timedelta('2 days ago')==2
#
# assert transform2timedelta('2 weeks ago')==14
#
# assert transform2timedelta('1 week ago')==7
#
# assert transform2timedelta('1 month ago')==30

# %% [markdown]
# ## prepare_users_df

# %%
def prepare_users_df(users_df,group_id=1818):
    """
    (df)-->df
    """
    users_df_prep=users_df.copy()
    users_df_prep['Last used']=users_df_prep['Last used'].apply(lambda x: transform2timedelta(x))
    users_df_prep['Joined']=users_df_prep['Joined'].map({'No':False,'Yes':True})
    users_df_prep=users_df_prep.rename(columns={
        'Last used':'days_since_last_login',
        'Joined':'joined_group',
        'Status':'group_activation',
        'Name':'user_name'
        })
    users_df_prep['group_id']=group_id
    users_df_prep['extraction_timestamp']=pd.Timestamp.today(tz='utc').strftime('%Y-%m-%d %H:%M:%S')
    users_df_prep.columns=[column.lower() for column in users_df_prep.columns]
    return users_df_prep


# %% [markdown]
# ## create gbq table with users_df

# %%
def create_users_table(project_id=project_id, file_path=file_path, file_name=file_name,table_name=users_table):
    """
    (str,str,str,str,str)-->gbq table
    """
    
    prepare_users_df(
        get_users_xlsx(
            project_id=project_id,
            file_path=file_path,
            file_name=file_name
        )).to_gbq(
            table_name,
            project_id=project_id,
            if_exists='append'
            )
    return "create_users_table"


# %% [markdown]
# ## start

# %%
def start(request=None):
    create_users_table()
    return "manual_users_table successfully created in GBQ"

# %% [markdown]
# # SCRIPTING

# %% tags=["active-ipynb"]
# start()

# %%
