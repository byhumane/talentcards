# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.4
#   kernelspec:
#     display_name: Python [conda env:std_env]
#     language: python
#     name: conda-env-std_env-py
# ---

# %% [markdown]
# # GOALS
# To create a GBQ view containing a daily state on users engagement.
# - generate dates index
# - generate users and dates index
# - fill last_login_date for each date/user key
# - fill last_consumption_start_date for each date/user key
# - fill last_consumption_completion_date for each date/user key

# %% [markdown]
# # PACKAGES

# %%
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import logging

# %% [markdown]
# # PARAMETERS

# %%
project_id = "analytics-dev-308300"

cred_file="../keys/gcp_key.json"

destination_table='raw_engagement.users_engagement'

# %%
pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)


# %% [markdown] tags=[]
# # FUNCTIONS

# %% [markdown]
# ## get data

# %%
def get_data(project_id, credentials):
    """
    ()-->df
    """
    query = """
        SELECT DISTINCT
            user_id,
            created_at,
            extraction_date,
            user_name
        FROM
            dtm_engagement.dim_users
        WHERE
            group_id=1818
    """
    users_df = pd.read_gbq(
        query=query, credentials=credentials, project_id=project_id)

    users_lst = users_df['user_id'].tolist()

    
    creation_df= users_df[['user_id','user_name','created_at']].drop_duplicates(ignore_index=True)
    creation_df['created_at']=pd.to_datetime(
        creation_df['created_at'], utc=True
        )

    query = """
        SELECT DISTINCT
            user_id,
            days_since_last_login,
            extraction_date,
            joined_group
        FROM
            dtm_engagement.hist_users
        WHERE
            group_id=1818
        ORDER BY
            extraction_date DESC
        """

    login_df = pd.read_gbq(
        query=query, credentials=credentials, project_id=project_id)

    login_df=login_df.drop_duplicates(subset=['user_id','extraction_date'],keep='last',ignore_index=True)
    
#     login_df["last_login"] = pd.to_datetime(
#         login_df["last_login"], utc=True
#     )

    query = """
        SELECT DISTINCT
            user_id,
            set_id,
            started_at,
            completed_at
        FROM
            dtm_engagement.ft_content_consumption
        WHERE
            group_id=1818
        """
    consumption_df = pd.read_gbq(
        query=query, credentials=credentials, project_id=project_id
    )

    consumption_df["started_at"] = pd.to_datetime(
        consumption_df["started_at"], utc=True
    )
    consumption_df["completed_at"] = pd.to_datetime(
        consumption_df["completed_at"], utc=True
    )

    return users_lst, creation_df, login_df, consumption_df


# %% [markdown]
# ## generate base (users and dates) data frame

# %%
def create_base_df(
    users_ls,
    creation_df,
    start_date="2021-08-16",
    end_date=pd.Timestamp.today().strftime("%Y-%m-%d"),
):
    """
    (date-like, date_like, series) --> df
    Create a dataframe with one row for each combination of user and date. Date range is defined by start_date and end_date (excluded).
    """
    dates_index = (
        pd.to_datetime(
            pd.date_range(start=start_date, end=end_date, name="action_date")
        )
        .strftime("%Y-%m-%d")
        .to_list()
    )

    actions_dict = [
        {"action_date": action_date, "user_id": user}
        for action_date in dates_index
        for user in users_ls
    ]
    
    base_df=pd.DataFrame(actions_dict)
    base_df=base_df.merge(creation_df, how='left',on='user_id')
    base_df=base_df.drop(index=base_df[base_df['created_at']>base_df['action_date']].index)
    
    return base_df


# %% [markdown]
# ## fill max date of interest for each date/user key

# %%
def max_date(consumption_df, reporting_date, user_id, date_of_interest):
    """
    (df,date like str, int)-->date
    Select the maximum value for date_of_interest field that is inferior to the reporting date (23:59:59), for the specified user_id.
    """
    max_start = consumption_df[
        (consumption_df["user_id"] == user_id)
        & (
            consumption_df[date_of_interest]
            <= pd.Timestamp(reporting_date + " 23:59:59", tz="UTC")
        )
    ][date_of_interest].max()

    return max_start


# %% [markdown] tags=[]
# ## Fill the number of completed sets by period

# %%
def calculate_nb_of_sets_of_interest(consumption_df,reporting_date,user_id,date_of_interest,nb_of_days):
    """
    (df, date like str, int, int, date like str)-->int
    For the user_id, count the number of set_ids where completed at is between reporting_date-number_of_days and reporting_date.
    """

    number_of_sets = consumption_df[(consumption_df['user_id']==user_id) & 
                                    (consumption_df[date_of_interest].between(pd.Timestamp(reporting_date + " 23:59:59", tz="UTC")-pd.Timedelta(nb_of_days,'days'),pd.Timestamp(reporting_date + " 23:59:59", tz="UTC")))]['set_id'].count()
    
    return number_of_sets


# %% [markdown] tags=[]
# ## Set user status based on dates

# %%
def user_status(days_since_last_login, days_since_last_start,days_since_last_completion,ever_logged):
    """
    (timedelta,timedelta,timedelta,bool)--> str
    """
    if ever_logged==False:
        return '0.bird'
    elif days_since_last_completion <= 7:
        return '4.learner'
    elif days_since_last_start <= 7:
        return '3.consumer'
    elif days_since_last_login <= 7:
        return '2.curious'
    else:
        return '1.missing'

# assert user_status(
#     timedelta_since_last_login=pd.Timedelta(pd.NaT),
#     timedelta_since_last_start=pd.Timedelta('2 days 13:41:36'),
#     timedelta_since_last_completion=pd.Timedelta('1 days 11:29:23'),
#     ever_logged=True)=='4.learner'

# assert user_status(
#     timedelta_since_last_login=pd.Timedelta(pd.NaT),
#     timedelta_since_last_start=pd.Timedelta('1 days 13:41:36'),
#     timedelta_since_last_completion=pd.Timedelta('7 days 11:29:23'),
#     ever_logged=True)=='3.consumer'

# assert user_status(
#     timedelta_since_last_login=pd.Timedelta(pd.NaT),
#     timedelta_since_last_start=pd.Timedelta('1 days 13:41:36'),
#     timedelta_since_last_completion=pd.Timedelta(pd.NaT),
#     ever_logged=True)=='3.consumer'

# # assert user_status(
# #     timedelta_since_last_login=pd.Timedelta('1 days 13:41:36'),
# #     timedelta_since_last_start=pd.Timedelta('8 days 13:41:36'),
# #     timedelta_since_last_completion=pd.Timedelta('7 days 11:29:23'),
# #     ever_logged=True)=='2.curious'

# assert user_status(
#     timedelta_since_last_login=pd.Timedelta('7 days 13:41:36'),
#     timedelta_since_last_start=pd.Timedelta('8 days 13:41:36'),
#     timedelta_since_last_completion=pd.Timedelta('10 days 11:29:23'),
#     ever_logged=True)=='1.missing'

# assert user_status(
#     timedelta_since_last_login=pd.Timedelta('0 days'),
#     timedelta_since_last_start=pd.Timedelta('0 days'),
#     timedelta_since_last_completion=pd.Timedelta('0 days'),
#     ever_logged=False)=='0.bird'


# %% [markdown]
# ## create engagement df by completing base_df with calculated fields

# %%
def generate_engagement_df(base_df,consumption_df,login_df):
    """
    (df,df,df)-->df
    """
    engagement_df=base_df.copy()
    
    engagement_df=pd.merge(engagement_df,login_df,left_on=['user_id','action_date'],right_on=['user_id','extraction_date'])
    
    engagement_df['last_start_date']=engagement_df.apply(lambda x: max_date(consumption_df,x['action_date'],x['user_id'],'started_at'), axis=1)
    engagement_df['timedelta_since_last_start']=pd.to_datetime(engagement_df['action_date']+' 23:59:59',utc=True)-engagement_df['last_start_date']
    engagement_df['days_since_last_start']=engagement_df['timedelta_since_last_start'].dt.days
    
    engagement_df['last_completion_date']=engagement_df.apply(lambda x: max_date(consumption_df,x['action_date'],x['user_id'],'completed_at'), axis=1)
    engagement_df['timedelta_since_last_completion']=(
        pd.to_datetime(engagement_df['action_date']+' 23:59:59',utc=True)-engagement_df['last_completion_date']
            )
    engagement_df['days_since_last_completion']=engagement_df['timedelta_since_last_completion'].dt.days
    
    engagement_df['nb_of_completed_sets']=engagement_df.apply(lambda x: calculate_nb_of_sets_of_interest(consumption_df=consumption_df,reporting_date=x['action_date'],user_id=x['user_id'],date_of_interest='completed_at',nb_of_days=7),axis=1)
    
    engagement_df['user_status']=engagement_df.apply(lambda x:
                                                               user_status(
                                                                   x['days_since_last_login'],
                                                                   x['days_since_last_start'],
                                                                   x['days_since_last_completion'],
                                                                   ever_logged=x['joined_group']),
                                                               axis=1
                                                              )
    
    return engagement_df


# %% [markdown]
# ## create user_engagement table in gbq

# %%
def create_engagement_table(project_id=project_id, destination_table_name=destination_table, cred_file=cred_file):
    """
    (df,str,str,str)--> gbq table
    """
    
    credentials = service_account.Credentials.from_service_account_file(cred_file)
    
    users_ls, creation_df, login_df, consumption_df = get_data(project_id=project_id,credentials=credentials)
    
    engagement_df = generate_engagement_df(
                                create_base_df(users_ls=users_ls,
                                               creation_df=creation_df),
                                consumption_df=consumption_df,
                                login_df=login_df
                    )
    
    engagement_df.to_gbq(destination_table_name,project_id=project_id,credentials=credentials,if_exists='replace')
    
    return engagement_df


# %% [markdown]
# # DATA WRANGLING

# %%
create_engagement_table()

# %%
credentials = service_account.Credentials.from_service_account_file(cred_file)
a,b,c,d = get_data(project_id=project_id, credentials=credentials)

# %%
c['timedelta_since_last_login'].to_datetime()

# %%
