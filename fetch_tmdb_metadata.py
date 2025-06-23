

# In[46]:


import pandas as pd 
import requests 
import os 
from datetime import datetime, timezone,timedelta
import pyarrow
import time
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

for var in ["user", "password", "account", "warehouse", "database", "schema", "API_KEY", "API_TOKEN", "schema_reporting"]:
    print(f"{var} →", "✅" if os.environ.get(var) else "❌ MISSING")

# Load API key from .env
API_KEY  = os.environ["API_KEY"]
LANGUAGE = "en-US"
BASE_URL = "https://api.themoviedb.org/3"
params = {"api_key": API_KEY}

# Load snowflake credentials from .env

conn = snowflake.connector.connect(
    user= os.environ["user"],
    password=os.environ["password"],
    account=os.environ["account"],
    warehouse=os.environ["warehouse"],
    database=os.environ["database"],
    schema=os.environ["schema"]
)


# In[47]:


def get_trending_show_ids():
    '''Extracts trending show ids from TMDB'''
    url =  f"{BASE_URL}/trending/tv/week?language=en-US"
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()["results"]
    trending_show_ids = [show['id'] for show in data]
    return trending_show_ids



# In[48]:


def get_trending_show_metadata(trending_show_id_list):
    '''Gets metadata of trending shows of the week and adds them to a list'''
    show_metadata = []
    for show in trending_show_id_list:
        try:
            url = f"{BASE_URL}/tv/{show}?"  
            response = requests.get(url, params=params)
            data = response.json()
            show_metadata.append(data)
        except requests.RequestException as error:
            print(f"Error fetching show {show}: {error}")
            continue
        time.sleep(0.2) 

    return show_metadata  


# In[49]:


def get_week_start_date():
    """Returns the Monday of the current week."""
    today = datetime.today().date()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    return start_of_week


# In[61]:


def extract_tv_show_to_df(show_metadata_list):
    ''' loads tv show metadata to df where each row represents a single show'''
    load_date = datetime.today().date()
    start_of_week = get_week_start_date()
    dataframe_rows =[]
    for show in show_metadata_list:
            dataframe_rows.append({
            "load_date": load_date,
            "start_of_week": start_of_week,
            "tv_show_id": show['id'],
            "first_air_date": show["first_air_date"],
            "tv_show_status": show['status'],
            "tv_show_type": show['type'],
            "vote_average": show['vote_average'],
            "original_language": show['original_language'],
            "original_name":show['original_name'],
            "localized_name": show['name'],
            "number_of_episodes":show["number_of_episodes"],
            "number_of_seasons":show["number_of_seasons"],
            "vote_count": show['vote_count'],
            "merge_key": f"{show['id']}_{start_of_week}",
           "record_load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None)



}
)
    df = pd.DataFrame(dataframe_rows)
    df.columns = [col.upper() for col in df.columns]
    return(df)



# In[59]:


#one show can have many genres and one genre will have many shows
'''loads all generes of a show to a dataframe where each row represnets a genre'''
def extract_genres_to_df(show_metadata_list):
    load_date = datetime.today().date()
    start_of_week = get_week_start_date()
    dataframe_rows = []
    for show in show_metadata_list:
        for genre in show.get("genres", []):
            dataframe_rows.append({
                "load_date": load_date,
                "week_start_date": start_of_week,
                "tv_show_id": show["id"],
                "genre_id": genre["id"],
                "genre_name": genre["name"],
                "merge_key": f"{show['id']}_{start_of_week}",
                "record_load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None)
})
    df = pd.DataFrame(dataframe_rows)
    df.columns = [col.upper() for col in df.columns]

    return(df)




# In[56]:


def extract_creators_to_df(show_metadata_list):
    '''loads all  creaters of a show to a dataframe where each row represents a creator'''
    dataframe_rows = []
    load_date =datetime.today().date()
    start_of_week = get_week_start_date()
    for show in show_metadata_list:
        for creator in show.get("created_by", []):
            dataframe_rows.append({
                "load_date":load_date,
                "week_start_date":start_of_week,
                "tv_show_id": show["id"],
                "creator_id": creator["id"],
                "creator_name": creator["name"],
                "merge_key": f"{show['id']}_{start_of_week}",
               "record_load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None)


            })
    df = pd.DataFrame(dataframe_rows)
    df.columns = [col.upper() for col in df.columns]
    return(df)


# In[58]:


def extract_networks_to_df(show_metadata_list):
    dataframe_rows = []
    load_date = datetime.today().date()
    start_of_week = get_week_start_date()
    for show in show_metadata_list:
        for network in show.get("networks", []):
            dataframe_rows.append({
                "load_date":load_date,
                "week_start_date":start_of_week,
                "tv_show_id": show["id"],
                "network_id": network["id"],
                "network_name": network["name"],
                "merge_key": f"{show['id']}_{start_of_week}",
                "record_load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None)

})
    df = pd.DataFrame(dataframe_rows)
    df.columns = [col.upper() for col in df.columns]

    return(df)


# In[57]:


def extracts_spoken_languages_to_df(show_metadata_list):
    dataframe_rows = []
    load_date = datetime.today().date()
    start_of_week = get_week_start_date()
    for show in show_metadata_list:
        for languages in show.get("spoken_languages", []):
            dataframe_rows.append({
                "load_date": load_date,
                "week_start_date":start_of_week,
                "tv_show_id": show["id"],
                "language_name": languages["english_name"],
                "merge_key": f"{show['id']}_{start_of_week}",
                "record_load_timestamp": datetime.now(timezone.utc).replace(tzinfo=None)

})
    df = pd.DataFrame(dataframe_rows)
    df.columns = [col.upper() for col in df.columns]
    return(df)



# In[62]:

def run_merge(conn, merge_sql):
    with conn.cursor() as cur:
        cur.execute(merge_sql)


def main():
    trending_show_id_list = get_trending_show_ids()
    show_metadata_list = get_trending_show_metadata(trending_show_id_list)
    tv_show_df = extract_tv_show_to_df(show_metadata_list)
    genre_df = extract_genres_to_df(show_metadata_list)
    creator_df = extract_creators_to_df(show_metadata_list)
    networks_df = extract_networks_to_df(show_metadata_list)
    spoken_language_df = extracts_spoken_languages_to_df(show_metadata_list)
    
    genre_df["RECORD_LOAD_TIMESTAMP"] = pd.to_datetime(genre_df["RECORD_LOAD_TIMESTAMP"])
    tv_show_df["RECORD_LOAD_TIMESTAMP"] = pd.to_datetime(tv_show_df["RECORD_LOAD_TIMESTAMP"])
    creator_df["RECORD_LOAD_TIMESTAMP"] = pd.to_datetime(creator_df["RECORD_LOAD_TIMESTAMP"])
    networks_df["RECORD_LOAD_TIMESTAMP"] = pd.to_datetime(networks_df["RECORD_LOAD_TIMESTAMP"])
    spoken_language_df["RECORD_LOAD_TIMESTAMP"] = pd.to_datetime(spoken_language_df["RECORD_LOAD_TIMESTAMP"])


    cur = conn.cursor()


    # Upload to Snowflake
    column_metadata = {"RECORD_LOAD_TIMESTAMP": "TIMESTAMP_NTZ"}
    cur.execute("TRUNCATE TABLE STG_TV_SHOWS") 
    write_pandas(conn, tv_show_df, 'STG_TV_SHOWS',use_logical_type=True)
    merge_sql_tv_show = """
    MERGE INTO tv_show AS target
    USING STG_TV_SHOWS AS source
    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN UPDATE SET
    target.record_load_timestamp = current_timestamp

    WHEN NOT MATCHED THEN INSERT (
    LOAD_DATE, 
    START_OF_WEEK,
    TV_SHOW_ID, 
    FIRST_AIR_DATE, 
    TV_SHOW_STATUS, 
    TV_SHOW_TYPE,
    VOTE_AVERAGE, 
    ORIGINAL_LANGUAGE,
    ORIGINAL_NAME, 
    NUMBER_OF_EPISODES, 
    NUMBER_OF_SEASONS, 
    VOTE_COUNT, 
    LOCALIZED_NAME, 
    MERGE_KEY, 
    RECORD_LOAD_TIMESTAMP 
    ) VALUES (
   source.LOAD_DATE, 
   source.START_OF_WEEK,
   source.TV_SHOW_ID, 
   source.FIRST_AIR_DATE, 
   source.TV_SHOW_STATUS, 
   source.TV_SHOW_TYPE,
   source.VOTE_AVERAGE, 
   source.ORIGINAL_LANGUAGE,
   source.ORIGINAL_NAME, 
   source.NUMBER_OF_EPISODES, 
   source.NUMBER_OF_SEASONS, 
   source.VOTE_COUNT, 
   source.LOCALIZED_NAME, 
   source.MERGE_KEY, 
   source.RECORD_LOAD_TIMESTAMP
    );
    """
    run_merge(conn, merge_sql_tv_show)
    cur.execute("TRUNCATE TABLE STG_GENRES")  
    write_pandas(conn, genre_df, 'STG_GENRES',use_logical_type=True)
    merge_sql_genres = """
    MERGE INTO genres AS target
    USING STG_GENRES AS source
    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN UPDATE SET
    target.record_load_timestamp = current_timestamp

    WHEN NOT MATCHED THEN INSERT (
    TV_SHOW_ID, 
    GENRE_ID, 
    GENRE_NAME, 
    LOAD_DATE,
    WEEK_START_DATE, 
    MERGE_KEY, 
    RECORD_LOAD_TIMESTAMP
    ) VALUES (
    source.TV_SHOW_ID, 
    source.GENRE_ID, 
    source.GENRE_NAME, 
    source.LOAD_DATE,
    source.WEEK_START_DATE, 
    source.MERGE_KEY, 
    source.RECORD_LOAD_TIMESTAMP
    );
    """
    run_merge(conn, merge_sql_genres)
    
    
    cur.execute("TRUNCATE TABLE STG_CREATORS")  
    write_pandas(conn, creator_df, 'STG_CREATORS',use_logical_type=True)
    merge_sql_creators = """
    MERGE INTO CREATORS AS target
    USING STG_CREATORS AS source
    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN UPDATE SET
    target.record_load_timestamp = current_timestamp

    WHEN NOT MATCHED THEN INSERT (
        TV_SHOW_ID, 
        CREATOR_ID, 
        CREATOR_NAME, 
        LOAD_DATE, 
        WEEK_START_DATE, 
        MERGE_KEY, 
        RECORD_LOAD_TIMESTAMP
    ) VALUES (
        source.TV_SHOW_ID, 
        source.CREATOR_ID, 
        source.CREATOR_NAME, 
        source.LOAD_DATE, 
        source.WEEK_START_DATE, 
        source.MERGE_KEY, 
        source.RECORD_LOAD_TIMESTAMP
                    );
                """
    run_merge(conn, merge_sql_creators)
    
    cur.execute("TRUNCATE TABLE STG_NETWORKS")  

    write_pandas(conn, networks_df, 'STG_NETWORKS',use_logical_type=True)
    merge_sql_networks = """
    MERGE INTO networks AS target
    USING STG_NETWORKS AS source
    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN UPDATE SET
    target.record_load_timestamp = current_timestamp

    WHEN NOT MATCHED THEN INSERT (
    TV_SHOW_ID, 
    NETWORK_ID, 
    NETWORK_NAME, 
    LOAD_DATE,
    WEEK_START_DATE, 
    MERGE_KEY, 
    RECORD_LOAD_TIMESTAMP
    ) VALUES (
    source.TV_SHOW_ID, 
    source.NETWORK_ID, 
    source.NETWORK_NAME, 
    source.LOAD_DATE,
    source.WEEK_START_DATE, 
    source.MERGE_KEY, 
    source.RECORD_LOAD_TIMESTAMP
    );
    """
    run_merge(conn, merge_sql_networks)
    
    cur.execute("TRUNCATE TABLE STG_SPOKEN_LANGUAGE")  

    write_pandas(conn, spoken_language_df, 'STG_SPOKEN_LANGUAGE',use_logical_type=True)
    merge_sql_spoken_language = """
    MERGE INTO spoken_language AS target
    USING STG_SPOKEN_LANGUAGE AS source
    ON target.merge_key = source.merge_key

    WHEN MATCHED THEN UPDATE SET
    target.record_load_timestamp = current_timestamp

    WHEN NOT MATCHED THEN INSERT (
    TV_SHOW_ID, 
    LANGUAGE_NAME, 
    LOAD_DATE,
    WEEK_START_DATE, 
    MERGE_KEY, 
    RECORD_LOAD_TIMESTAMP
    ) VALUES (
    source.TV_SHOW_ID, 
    source.LANGUAGE_NAME, 
    source.LOAD_DATE,
    source.WEEK_START_DATE, 
    source.MERGE_KEY, 
    source.RECORD_LOAD_TIMESTAMP
    );
    """
    run_merge(conn, merge_sql_spoken_language)
    print('ran everything')


if __name__ == "__main__":
    main()


# 
