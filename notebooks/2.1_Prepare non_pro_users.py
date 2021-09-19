# ---
# jupyter:
#   jupytext:
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
# Identify non professional TalentCards users in GBQ.

# %% [markdown]
# # PACKAGES

# %%
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq
import yaml

# %% [markdown]
# # PARAMETERS

# %%
project_id = "analytics-dev-308300"

credentials = service_account.Credentials.from_service_account_file(
    "../keys/gcp_key.json",
)

# %% [markdown]
# # FUNCTIONS

# %%

# %% [markdown]
# # DATA WRANGLING

# %% [markdown]
# CSV file was manually generated:
# - extract list of non-pro users from TalentCards (filter)
# - manipulate the excel file to include the assotiation and clean any data besides identifier and association
# - export as csv

# %%
df_non_pro=pd.read_csv('../params/non_professional_users.csv',sep=';')
df_non_pro['Identifier']=df_non_pro['Identifier'].replace({'-':''},regex=True)
df_non_pro

# %%
query = """
SELECT DISTINCT
    user_id, access_token
FROM
    dtm_engagement.dim_users
"""

df_bq_users=pd.read_gbq(query=query, credentials=credentials, project_id=project_id)

# %%
df_bq_users_flagged=(
    df_bq_users.merge(df_non_pro,left_on='access_token',right_on='Identifier')
    .drop(columns=['Identifier','access_token'])
    .rename(columns={'Association':'association'}))
df_bq_users_flagged

# %%
df_bq_users_flagged.to_gbq('talentcards.non_pro_users',if_exists='replace')

# %%
