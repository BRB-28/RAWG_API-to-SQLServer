import boto3
from io import BytesIO
import json
import pandas as pd
import numpy as np
from config import conn_str, BUCKET_NAME, S3_KEY


# Download file from S3 into memory
s3 = boto3.client('s3')
json_buffer = BytesIO()
s3.download_fileobj(BUCKET_NAME, S3_KEY, json_buffer)
json_buffer.seek(0)  # Reset buffer position

# Load JSON from buffer
data = json.load(json_buffer)
games = data['results']

print(f"Loaded {len(games)} games from S3 file")

games_df = pd.json_normalize(games)

games_table = games_df[[
    'id', 'slug', 'name', 'released', 'tba', 'background_image',
    'rating', 'rating_top', 'ratings_count', 'metacritic', 'playtime',
    'suggestions_count', 'updated', 'esrb_rating.id', 'esrb_rating.name'
]].rename(columns={
    'id': 'game_id',
    'esrb_rating.id': 'esrb_rating_id',
    'esrb_rating.name': 'esrb_rating_name'
})

platform_records = []
for game in games:
    for platform in game['platforms']:
        platform_id = platform['platform']['id']
        platform_name = platform['platform']['name']
        platform_records.append({'platform_id': platform_id, 'name': platform_name})

platforms_df = pd.DataFrame(platform_records).drop_duplicates()

game_platforms = []
for game in games:
    game_id = game['id']
    for platform in game['platforms']:
        platform_id = platform['platform']['id']
        game_platforms.append({'game_id': game_id, 'platform_id': platform_id})

game_platforms_df = pd.DataFrame(game_platforms).drop_duplicates()

genre_records = []
game_genres = []

for game in games:
    game_id = game['id']
    for genre in game['genres']:
        genre_id = genre['id']
        genre_name = genre['name']
        genre_records.append({'genre_id': genre_id, 'name': genre_name})
        game_genres.append({'game_id': game_id, 'genre_id': genre_id})

genres_df = pd.DataFrame(genre_records).drop_duplicates()
game_genres_df = pd.DataFrame(game_genres).drop_duplicates()

store_records = []
game_stores = []

for game in games:
    game_id = game['id']
    for store in game['stores']:
        store_id = store['store']['id']
        store_name = store['store']['name']
        store_records.append({'store_id': store_id, 'name': store_name})
        game_stores.append({'game_id': game_id, 'store_id': store_id})

stores_df = pd.DataFrame(store_records).drop_duplicates()
game_stores_df = pd.DataFrame(game_stores).drop_duplicates()

tag_records = []
game_tags = []

for game in games:
    game_id = game['id']
    for tag in game['tags']:
        tag_id = tag['id']
        tag_name = tag['name']
        tag_records.append({'tag_id': tag_id, 'name': tag_name})
        game_tags.append({'game_id': game_id, 'tag_id': tag_id})

tags_df = pd.DataFrame(tag_records).drop_duplicates()
game_tags_df = pd.DataFrame(game_tags).drop_duplicates()

screenshots = []

for game in games:
    game_id = game['id']
    for shot in game['short_screenshots']:
        screenshot_id = shot['id']
        image_url = shot['image']
        screenshots.append({'screenshot_id': screenshot_id, 'game_id': game_id, 'image_url': image_url})

screenshots_df = pd.DataFrame(screenshots).drop_duplicates()


## Start the SQL Connection
import pyodbc

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

def insert_dataframe(df, table_name, conn):
    cursor = conn.cursor()
    columns = ','.join(df.columns)
    placeholders = ','.join(['?'] * len(df.columns))
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    for index, row in df.iterrows():
        # print(f"Inserting into {table_name}, row {index + 1}/{len(df)}")
        # if index == 23:  # Python 0-based index, so row 24 is index 23
            # print("----- DEBUGGING ROW 24 -----")
            # print(row)
            # print(row.dtypes)
            # print(tuple(row.values))

        clean_row = [int(val) if isinstance(val, (np.integer,)) else 
             float(val) if isinstance(val, (np.floating,)) else 
             str(val) if isinstance(val, (np.str_,)) else 
             val for val in row.values]

        cursor.execute(sql, clean_row)

        #print(f"SQL: {sql}")
        #print(f"Row tuple: {tuple(row.values)}")
        #print(f"Tuple length: {len(tuple(row.values))}")
    
    conn.commit()
    print(f"Inserted {len(df)} rows into {table_name}")


if __name__ == '__main__':
    delete_statements = [
        "DELETE FROM GamePlatforms;",
        "DELETE FROM Platforms;",
        "DELETE FROM GameGenres;",
        "DELETE FROM Genres;",
        "DELETE FROM GameStores;",
        "DELETE FROM Stores;",
        "DELETE FROM GameTags;",
        "DELETE FROM Tags;",
        "DELETE FROM Screenshots;",
        "DELETE FROM Games;"
    ]

    for stmt in delete_statements:
        cursor = conn.cursor()
        cursor.execute(stmt)
        conn.commit()
        print(f"Deleted rows from {stmt.split()[2]}")

    # Ensure numeric, coerce bad values, and fill NaNs
    games_table['rating'] = pd.to_numeric(games_table['rating'], errors='coerce').fillna(0).astype(float)
    games_table['rating_top'] = pd.to_numeric(games_table['rating_top'], errors='coerce').fillna(0).astype(float)
    games_table['esrb_rating_id'] = games_table['esrb_rating_id'].fillna(0).astype(int)
    games_table['ratings_count'] = games_table['ratings_count'].fillna(0).astype(int)
    games_table['metacritic'] = games_table['metacritic'].fillna(0).astype(int)
    games_table['playtime'] = games_table['playtime'].fillna(0).astype(int)
    games_table['suggestions_count'] = games_table['suggestions_count'].fillna(0).astype(int)
    games_table['esrb_rating_name'] = games_table['esrb_rating_name'].fillna('')
    game_platforms_df = game_platforms_df.astype({'game_id': int, 'platform_id': int})


    insert_dataframe(games_table, 'Games', conn)
    insert_dataframe(platforms_df, 'Platforms', conn)
    insert_dataframe(game_platforms_df, 'GamePlatforms', conn)
    insert_dataframe(genres_df, 'Genres', conn)
    insert_dataframe(game_genres_df, 'GameGenres', conn)
    insert_dataframe(stores_df, 'Stores', conn)
    insert_dataframe(game_stores_df, 'GameStores', conn)
    insert_dataframe(tags_df, 'Tags', conn)
    insert_dataframe(game_tags_df, 'GameTags', conn)

    screenshots_df = screenshots_df.drop_duplicates(subset='screenshot_id')
    insert_dataframe(screenshots_df, 'Screenshots', conn)

    conn.close()
    print("All inserts completed")

