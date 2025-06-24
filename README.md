# TMDB Trending Show DBT Project 

## 📌 Project Overview 

This project simulates how a content strategy team at a streaming company might analyze trending TV show data to guide investment decisions.
Using the TMDB API, it ingests weekly trending show metadata, transforms the data into business-ready reporting models using dbt, and delivers high-level insights through an interactive Streamlit app.
Tools used: Python, Snowflake, dbt Cloud, and Streamlit.

## 📊 Data Pipeline
Source: TMDB Trending TV Show API — provides weekly metadata on trending TV shows

Ingestion: A Python script extracts and loads the data into Snowflake. This process is automated using GitHub Actions.

Transformation: dbt Cloud is used to model the raw data into clean staging tables, fact/dimension models, and reporting outputs. dbt jobs are scheduled to keep models up to date.

Reporting: A Streamlit app presents interactive insights based on the modeled data.

## 🧱 dbt Models
The project uses a multi-layered dbt structure to organize transformations and support clear business logic:

### staging
Each model here maps one-to-one with a raw Snowflake table. Only light transformations are applied — such as column renaming and data type formatting — to prepare the data for downstream modeling.

### intermediate/facts_and_dims 
This layer includes a star schema anchored by fct_weekly_trending_tv_shows, which represents each weekly instance of a trending TV show. Supporting dimension tables (such as dim_genres and dim_creators) provide descriptive context.

### intermediate/bridge
Models in this folder handle many-to-many relationships. For example, a single TV show can belong to multiple genres, and a genre can appear across many shows. These bridge tables normalize that complexity for easier analysis.

### reporting
This layer contains business-ready tables designed for consumption by analysts and end users. For example, reporting_weekly_network_insights aggregates weekly metrics like show counts and popularity scores by network.

## 🧪 Data Quality & Testing
Data quality is enforced through a robust suite of dbt tests:

  not_null tests are applied to all columns across all models to ensure there is no missing or incomplete data.
  
  unique tests are applied to primary key fields in dimension models, such as creator_id or genre_id, to guarantee entity uniqueness.
  
  accepted_values tests are used for fields with static, controlled values — for example, ensuring that tv_show_status only contains "Returning" or "Ended".
  
  These tests help ensure the trustworthiness of reporting layers and support downstream tools like dashboards and visualizations.

##  📸 Streamlit App
To demonstrate the usability of the reporting models, this project includes a lightweight Streamlit app that surfaces key metrics and trends. Each reporting model is visualized with at least one corresponding chart or table, allowing users to explore the insights interactively.

The app is designed to showcase how data teams or business users might interact with these models in a real-world setting.

## 🧠 Lessons Learned & Next Steps

This project helped solidify my understanding of modern analytics engineering workflows — including data modeling with dbt, enforcing data quality through testing, and automating pipelines using GitHub Actions and dbt Cloud.

It was also my first time working with a public API, which I used to ingest weekly metadata on trending TV shows. I wrote a Python ingestion script to connect to the TMDB API, transform the results into clean DataFrames, and load the data into Snowflake, where the rest of my modeling pipeline takes over.

On the transformation side, I deepened my understanding of:

Star schema design, with a fact table (fct_weekly_trending_tv_shows) and supporting dimensions like genres and creators

SCD Type 2 logic, using surrogate keys and historical tracking to capture changes in dimension attributes over time

While this project uses full-refresh runs for simplicity and clarity, my next project will focus on more advanced data engineering practices, including:

Production-ready incremental models in dbt

Scalable, high-volume data ingestion pipelines

Deeper use of GCP-native tools for orchestration and storage

