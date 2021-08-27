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
logger = logging.getLogger("pandas_gbq")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

project_id = "analytics-dev-308300"

credentials = service_account.Credentials.from_service_account_file(
    "../keys/gcp_key.json",
)

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)


# %% [markdown] tags=[]
# # FUNCTIONS

# %% [markdown]
# ## get data

# %%
def get_data():
    """
    ()-->df
    """
    query = """
        SELECT DISTINCT
            user_id, created_at, extraction_date
        FROM
            dtm_engagement.dim_users
        WHERE
            group_id=1818
    """
    users_df = pd.read_gbq(
        query=query, credentials=credentials, project_id=project_id)

    users_lst = users_df['user_id'].tolist()

    
    creation_df= users_df[['user_id','created_at']].drop_duplicates(ignore_index=True)
    creation_df['created_at']=pd.to_datetime(
        creation_df['created_at'], utc=True
        )

    query = """
        SELECT DISTINCT
            user_id, last_login, extraction_date
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

    query = """
        SELECT DISTINCT
            user_id,
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
    ls_users,
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
        for user in ls_users
    ]
    
    base_df=pd.DataFrame(actions_dict)
    base_df=base_df.merge(creation_df, how='left',on='user_id')
    base_df=base_df.drop(index=base_df[base_df['created_at']>base_df['action_date']].index)
    
    return base_df


# %% [markdown]
# ## fill last_login_date for each date/user key

# %%
def update_last_login(base_df, logins_df):
    """
    (df,df)-->df
    Include last_login and created_at into actions_df.
    """
    actions_df = base_df.merge(
        logins_df,
        how="left",
        left_on=["action_date", "user_id"],
        right_on=["extraction_date", "user_id"],
    ).drop(columns=["extraction_date"])
    
    actions_df['last_login']=actions_df['last_login'].fillna(pd.NaT)
    
    return actions_df


# %% [markdown]
# ## fill last_consumption_start_date for each date/user key

# %%
def max_date(consumption_df, reporting_date, user_id, date_of_interest):
    """
    (df,date like str, int)
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


# %% [markdown]
# ## update consumption dates

# %%
def update_consumption_dates(actions_df,consumption_df):
    """
    (df,df)-->df
    Update the start_date for each user with the maximum value inferior to the action_date. Action date is extended with 23:59:59 to encompass the entire day.
    """
    updated_actions_df=actions_df.copy()
    updated_actions_df['last_start_date']=updated_actions_df.apply(lambda x: max_date(consumption_df,x['action_date'],x['user_id'],'started_at'), axis=1)
    updated_actions_df['last_completion_date']=updated_actions_df.apply(lambda x: max_date(consumption_df,x['action_date'],x['user_id'],'completed_at'), axis=1)
    
    updated_actions_df['timedelta_since_last_login']=pd.to_datetime(updated_actions_df['action_date']+' 23:59:59',utc=True)-pd.to_datetime(updated_actions_df['last_login'],utc=True)
    updated_actions_df['timedelta_since_last_start']=pd.to_datetime(updated_actions_df['action_date']+' 23:59:59',utc=True)-updated_actions_df['last_start_date']
    updated_actions_df['timedelta_since_last_completion']=pd.to_datetime(updated_actions_df['action_date']+' 23:59:59',utc=True)-updated_actions_df['last_completion_date']
    
    updated_actions_df['user_status']=updated_actions_df.apply(lambda x:
                                                               user_status(
                                                                   x['timedelta_since_last_login'],
                                                                   x['timedelta_since_last_start'],
                                                                   x['timedelta_since_last_completion']),
                                                               axis=1
                                                              )
    
    return updated_actions_df
    



# %% [markdown]
# ## Set user status based on dates

# %%
def user_status(timedelta_since_last_login, timedelta_since_last_start,timedelta_since_last_completion):
    """
    (timedelta,timedelta,timedelta)--> str
    """
    if timedelta_since_last_completion <= pd.Timedelta(7,'D'):
        return '4.learner'
    elif timedelta_since_last_start <= pd.Timedelta(7,'D'):
        return '3.consumer'
    elif timedelta_since_last_login <= pd.Timedelta(7,'D'):
        return '2.curious'
    else:
        return '1.missing'

assert user_status(
    timedelta_since_last_login=pd.Timedelta(pd.NaT),
    timedelta_since_last_start=pd.Timedelta('2 days 13:41:36'),
    timedelta_since_last_completion=pd.Timedelta('1 days 11:29:23'))=='4.learner'

assert user_status(
    timedelta_since_last_login=pd.Timedelta(pd.NaT),
    timedelta_since_last_start=pd.Timedelta('1 days 13:41:36'),
    timedelta_since_last_completion=pd.Timedelta('7 days 11:29:23'))=='3.consumer'

assert user_status(
    timedelta_since_last_login=pd.Timedelta(pd.NaT),
    timedelta_since_last_start=pd.Timedelta('1 days 13:41:36'),
    timedelta_since_last_completion=pd.Timedelta(pd.NaT))=='3.consumer'

assert user_status(
    timedelta_since_last_login=pd.Timedelta('1 days 13:41:36'),
    timedelta_since_last_start=pd.Timedelta('8 days 13:41:36'),
    timedelta_since_last_completion=pd.Timedelta('7 days 11:29:23'))=='2.curious'

assert user_status(
    timedelta_since_last_login=pd.Timedelta('7 days 13:41:36'),
    timedelta_since_last_start=pd.Timedelta('8 days 13:41:36'),
    timedelta_since_last_completion=pd.Timedelta('10 days 11:29:23'))=='1.missing'

# %% [markdown]
# # DATA WRANGLING

# %%
ls_users, df_creation, df_logins, df_consumption = get_data()

# %%
df_actions = update_last_login(create_base_df(ls_users,df_creation), df_logins)

# %%
df_actions_final=update_consumption_dates(df_actions,df_consumption)
df_actions_final

# %%
df_actions_final.to_gbq('dtm_engagement.ft_users_engagement',project_id=project_id,if_exists='replace',credentials=credentials)

# %%
