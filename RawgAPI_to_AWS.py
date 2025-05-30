import requests
import boto3
import json
from io import BytesIO
from config import API_KEY, BASE_URL, BUCKET_NAME, S3_PREFIX

# ---------- CONFIG ---------- #
total_pages = 5 #40 on each page

all_results = []

for page in range(1, total_pages + 1):
# ---------- API CALL ---------- #
    params = {
        'key': API_KEY,
        'page_size': 40,  # start small
        'page': page
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        data = response.json()
        all_results.extend(data['results'])
        print(f"Fetched page {page} with {len(data['results'])} games")
    else:
        print(f"Error on page {page}: {response.status_code} - {response.text}")

final_data = {'results': all_results}

# ---------- CONVERT TO BYTE STREAM ---------- #
json_bytes = BytesIO(json.dumps(final_data).encode('utf-8'))

# ---------- UPLOAD TO S3 ---------- #
s3 = boto3.client('s3')
s3_key = f'{S3_PREFIX}rawg_games.json'
s3.upload_fileobj(json_bytes, BUCKET_NAME, s3_key)

print(f'Successfully uploaded API response to s3://{BUCKET_NAME}/{s3_key}')




