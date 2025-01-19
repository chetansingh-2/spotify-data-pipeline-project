import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def artist(data):
    artist_list = []
    for row in data['items']:
        for key, values in row.items():
            if key == 'track':
                for artist in values['artists']:
                    artist_dict = {'artist_id': artist['id'], 'artist_name': artist['name'], 'artist_url': artist['href']}
                    artist_list.append(artist_dict)
    return artist_list

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_urls = row['track']['album']['external_urls']['spotify']
        album_image = row['track']['album']['images'][0]['url']
        album_element = {
            'album_id': album_id,
            'album_name': album_name,
            'album_release_date': album_release_date,
            'album_total_tracks': album_total_tracks,
            'album_urls': album_urls,
            'album_image': album_image
        }
        album_list.append(album_element)
    return album_list

def lambda_handler(event, context):
    client = boto3.client('s3')
    Bucket = "spotify-etl-pipeline-chetan"
    Key = "raw-data/to_process/"

    spotify_data_list = []
    spotify_keys = []
    for file in client.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file = file['Key']
        if file.split('.')[-1] == 'json':
            response = client.get_object(Bucket=Bucket, Key=file)
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            spotify_data_list.append(data)
            spotify_keys.append(file)

    for data in spotify_data_list:
        artist_list = artist(data)
        album_list = album(data)

        album_df = pd.DataFrame(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])
        album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'], errors='coerce')
        album_df = album_df.reset_index(drop=True)  # Ensures no index is added

        artist_df = pd.DataFrame(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])
        artist_df = artist_df.reset_index(drop=True)  # Ensures no index is added

        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        album_key = f"transformed-data/album-data/{timestamp}.csv"
        artist_key = f"transformed-data/artist-data/{timestamp}.csv"

        # Convert to CSV and upload
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        album_content = album_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=album_key, Body=album_content)

        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        client.put_object(Bucket=Bucket, Key=artist_key, Body=artist_content)

    # Move processed files to 'processed-data' folder
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': Bucket, 'Key': key}
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw-data/processed-data/' + key.split('/')[-1])
        s3_resource.Object(Bucket, key).delete()
