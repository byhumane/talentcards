from typing import List

import pandas as pd


def get_users_data():
    users_query = """
            SELECT DISTINCT
                user_id, 
                created_at, 
                extraction_date,
                CONCAT(first_name, ' ', last_name) as user_name
            FROM
                dtm_engagement.dim_users
            WHERE
                group_id=1818
        """
    return pd.read_gbq(query=users_query)


def get_users_ids(users_df: pd.DataFrame) -> List[int]:
    return users_df["user_id"].tolist()


def get_creation_data(users_df: pd.DataFrame) -> pd.DataFrame:
    creation_df = users_df[["user_id", "user_name", "created_at"]].drop_duplicates(
        ignore_index=True
    )
    creation_df["created_at"] = pd.to_datetime(creation_df["created_at"], utc=True)
    return creation_df


def get_login_data() -> pd.DataFrame:
    login_query = """
            SELECT DISTINCT
                user_id, last_login, extraction_date
            FROM
                dtm_engagement.hist_users
            WHERE
                group_id=1818
            ORDER BY
                extraction_date DESC
            """
    login_df = pd.read_gbq(query=login_query)
    login_df = login_df.drop_duplicates(
        subset=["user_id", "extraction_date"], keep="last", ignore_index=True
    )
    login_df["last_login"] = pd.to_datetime(login_df["last_login"], utc=True)
    return login_df


def get_consumption_data() -> pd.DataFrame:
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
    consumption_df = pd.read_gbq(query=query)

    consumption_df["started_at"] = pd.to_datetime(
        consumption_df["started_at"], utc=True
    )
    consumption_df["completed_at"] = pd.to_datetime(
        consumption_df["completed_at"], utc=True
    )
    return consumption_df


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
    base_df = pd.DataFrame(actions_dict)
    base_df = base_df.merge(creation_df, how="left", on="user_id")
    base_df = base_df.drop(
        index=base_df[base_df["created_at"] > base_df["action_date"]].index
    )
    return base_df


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


def calculate_nb_of_sets_of_interest(
    consumption_df, reporting_date, user_id, date_of_interest, nb_of_days
):
    """
    (df, date like str, int, int, date like str)-->number
    For the user_id, count the number of set_ids where completed at is between reporting_date-number_of_days and reporting_date.
    """

    number_of_sets = consumption_df[
        (consumption_df["user_id"] == user_id)
        & (
            consumption_df[date_of_interest].between(
                pd.Timestamp(reporting_date + " 23:59:59", tz="UTC")
                - pd.Timedelta(nb_of_days, "days"),
                pd.Timestamp(reporting_date + " 23:59:59", tz="UTC"),
            )
        )
    ]["set_id"].count()
    return number_of_sets


def user_status(
    timedelta_since_last_login,
    timedelta_since_last_start,
    timedelta_since_last_completion,
):
    """
    (timedelta,timedelta,timedelta)--> str
    """
    if timedelta_since_last_completion <= pd.Timedelta(7, "D"):
        return "4.learner"
    elif timedelta_since_last_start <= pd.Timedelta(7, "D"):
        return "3.consumer"
    elif timedelta_since_last_login <= pd.Timedelta(7, "D"):
        return "2.curious"
    else:
        return "1.missing"


def generate_engagement_df(base_df, consumption_df, login_df):
    """
    (df,df)-->df
    Update the start_date for each user with the maximum value inferior to the action_date. Action date is extended with 23:59:59 to encompass the entire day.
    """
    engagement_df = base_df.copy()
    engagement_df["last_login_date"] = engagement_df.apply(
        lambda row: max_date(
            login_df, row["action_date"], row["user_id"], "last_login"
        ),
        axis=1,
    )
    engagement_df["timedelta_since_last_login"] = pd.to_datetime(
        engagement_df["action_date"] + " 23:59:59", utc=True
    ) - pd.to_datetime(engagement_df["last_login_date"], utc=True)
    engagement_df["days_since_last_login"] = engagement_df[
        "timedelta_since_last_login"
    ].dt.days
    engagement_df["last_start_date"] = engagement_df.apply(
        lambda row: max_date(
            consumption_df, row["action_date"], row["user_id"], "started_at"
        ),
        axis=1,
    )
    engagement_df["timedelta_since_last_start"] = (
        pd.to_datetime(engagement_df["action_date"] + " 23:59:59", utc=True)
        - engagement_df["last_start_date"]
    )
    engagement_df["days_since_last_start"] = engagement_df[
        "timedelta_since_last_start"
    ].dt.days
    engagement_df["last_completion_date"] = engagement_df.apply(
        lambda row: max_date(
            consumption_df, row["action_date"], row["user_id"], "completed_at"
        ),
        axis=1,
    )
    engagement_df["timedelta_since_last_completion"] = (
        pd.to_datetime(engagement_df["action_date"] + " 23:59:59", utc=True)
        - engagement_df["last_completion_date"]
    )
    engagement_df["days_since_last_completion"] = engagement_df[
        "timedelta_since_last_completion"
    ].dt.days
    engagement_df["nb_of_completed_sets"] = engagement_df.apply(
        lambda row: calculate_nb_of_sets_of_interest(
            consumption_df=consumption_df,
            reporting_date=row["action_date"],
            user_id=row["user_id"],
            date_of_interest="completed_at",
            nb_of_days=7,
        ),
        axis=1,
    )

    engagement_df["user_status"] = engagement_df.apply(
        lambda row: user_status(
            row["timedelta_since_last_login"],
            row["timedelta_since_last_start"],
            row["timedelta_since_last_completion"],
        ),
        axis=1,
    )
    return engagement_df


def start(request):
    users_df = get_users_data()
    users_ids = get_users_ids(users_df)
    df_creation = get_creation_data(users_df)
    df_login = get_login_data()
    df_consumption = get_consumption_data()
    df_engagement = generate_engagement_df(
        create_base_df(users_ids, df_creation), df_consumption, df_login
    )
    talentcards_dataset = "raw_engagement"
    users_table_name = "users_engagement_beta"
    df_engagement.to_gbq(
        f"{talentcards_dataset}.{users_table_name}",
        if_exists="replace",
        progress_bar=True,
    )
    return "Function talentcard-users-engagement-to-bq finished successfully!"
