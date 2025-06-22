# %%
import os
import pandas as pd
from dotenv import load_dotenv
import snowflake.connector
import streamlit as st
import streamlit as st
import pandas as pd
import plotly.express as px
load_dotenv()

# creating query statement
def run_query(query):
    conn = snowflake.connector.connect(
        user= os.getenv("user"),
    password=os.getenv("password"),
    account=os.getenv("account"),
    warehouse=os.getenv("warehouse"),
    database=os.getenv("database"),
    schema=os.getenv("schema_reporting")
    )
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    conn.close()
    return df

# %%
# load dataframes
def load_table(table_name):
    return run_query(f"SELECT * FROM {table_name}")

creator_df= load_table('REPORTING_CREATOR_INSIGHTS')
genre_df = load_table('REPORTING_GENRE_INSIGHTS_SUMMARY')
tv_show_df = load_table('REPORTING__WEEKLY_TV_SHOW_PERFORMANCE')
networks_df = load_table('REPORTING_NETWORK_INSIGHTS_SUMMARY')
spoken_df = load_table('REPORTING_SPOKEN_LANGUAGES')



# %%
# creating filters for week 
st.sidebar.title("Filters")
selected_week = st.sidebar.selectbox("Select Trending Week", tv_show_df['TRENDING_WEEK_START_DATE'].unique())
selected_language = st.sidebar.selectbox("Select Show Language Code", tv_show_df['ORIGINAL_LANGUAGE_CODE'].unique())


filtered_df = tv_show_df[(tv_show_df['TRENDING_WEEK_START_DATE'] == selected_week)
& (tv_show_df['ORIGINAL_LANGUAGE_CODE'] == selected_language)]


# %%
# Header
st.title("📺 TMDB TV Show Trending Dashboard")

# KPIs
st.metric("Total Shows", len(filtered_df))
st.metric("Avg Vote", round(filtered_df['LIFETIME_VOTE_AVERAGE'].mean(), 2))
st.metric('Vote Count',filtered_df['LIFETIME_VOTE_COUNT'].sum())

# %%
st.header("🔥 Trending Shows")
fig_trending = px.bar(filtered_df.sort_values('LIFETIME_VOTE_COUNT', ascending=False).head(10),
                      x='LOCALIZED_NAME', y='LIFETIME_VOTE_COUNT',title='Top 10 Most Voted Shows')

# Update axis labels and layout
fig_trending.update_layout(
    xaxis_title='TV Show Name',
    yaxis_title='Lifetime Vote Count',
    title_x=0.5
)
st.plotly_chart(fig_trending)


# %%
# Creator Section
st.header("🎬 Top Creators")
fig_creators = px.bar(creator_df.sort_values('CREATOR_VOTING_AVERAGE', ascending=False).head(10),
                      x='CREATOR_NAME', y='CREATOR_VOTING_AVERAGE', title = 'Top 10 Creators')

# Update axis labels and layout
fig_creators.update_layout(
    xaxis_title='Creator Name',
    yaxis_title='Average Rating',
    title_x=0.5
)
st.plotly_chart(fig_creators)


# %%
# Language Section
st.header("🌎 Language Distribution")
fig_lang = px.bar(spoken_df, x='LANGUAGE_NAME', y='NUMBER_OF_TV_SHOWS', title = 'Language Popularity')
# Update axis labels and layout
fig_lang.update_layout(
    xaxis_title='Language',
    yaxis_title='TV Show Count',
    title_x=0.5
)
st.plotly_chart(fig_lang)

# %%
# Genre Section
st.header("🎭 Genre Distribution")
fig_genre = px.bar(genre_df, x='GENRE_NAME', y='GENRE_SHOW_COUNT', title = 'Genre Popularity')
# Update axis labels and layout
fig_genre.update_layout(
    xaxis_title='Genre Name',
    yaxis_title='TV Show Count',
    title_x=0.5
)
st.plotly_chart(fig_genre)


# %%
# Networks
st.header("📡 Networks")
fig_networks = px.bar(networks_df.sort_values('NETWORK_VOTE_COUNT', ascending=False).head(10),
                      x='NETWORK_NAME', y='NETWORK_VOTE_COUNT', title ='Network Popularity')

# Update axis labels and layout
fig_networks.update_layout(
    xaxis_title='Network Name',
    yaxis_title='Vote Count',
    title_x=0.5
)
st.plotly_chart(fig_networks)

# %%



