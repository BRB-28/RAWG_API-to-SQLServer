RAWG Video Game API → S3 → SQL Pipeline

This project builds a full data engineering pipeline that:

- Pulls video game data from the RAWG Video Games API
- Uploads raw JSON data to an AWS S3 bucket
- Transforms and flattens the data using Python (pandas)
- Loads the cleaned data into a local SQL Server database for long-term storage and analytics

Pipeline Overview

- API Ingestion:
  Uses requests to pull multi-page game data from RAWG API, handling pagination (up to 40 results per page).

- Cloud Storage:
  Uploads raw API responses to an AWS S3 bucket using boto3.

- Data Transformation:
  Normalizes nested JSON with pandas and breaks it into relational tables (Games, Platforms, Genres, Stores, Tags, Screenshots).

- Database Load:
  Uses pyodbc to connect to SQL Server and insert cleaned tables with proper primary/foreign key relationships.

Tech Stack

- Python (requests, pandas, boto3, pyodbc, numpy)
- AWS S3
- SQL Server
- Git and GitHub

How to Run

1. Set up your own config.py:
   See config_template.py for the required fields.

2. Run the two scripts:
   API to S3 Upload:
     python RawgAPI_to_AWS.py
   S3 to SQL Load:
     python TransformJSON.py




