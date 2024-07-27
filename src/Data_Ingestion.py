import requests
import time
import os
import boto3
import json
from io import StringIO

# List of simple to collect features
snippet_features = ["title" ]

# Any characters to exclude, generally these are things that become problematic in CSV files
unsafe_characters = ['\n', '"']

# Used to identify columns, currently hardcoded order
header = ["video_id"] + ["trending_date", "title" , "channelTitle", "categoryId","publish_time", "tags", "views", "likes", "dislikes",
                                            "comment_count", "thumbnail_link", "comments_disabled",
                                            "ratings_disabled","video_error_or_removed" ,"description"]

# Initialize S3 client
s3_client = boto3.client('s3')


def prepare_feature(feature):
    # Removes any character from the unsafe characters list and surrounds the whole item in quotes
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(page_token, country_code, api_key):
    # Builds the URL and requests the JSON from it
    request_url = (f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}"
                   f"chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}")
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        raise Exception("Rate limit exceeded")
    return request.json()


def get_tags(tags_list):
    # Takes a list of tags, prepares each tag and joins them into a string by the pipe character
    return prepare_feature("|".join(tags_list))


def get_videos(items):
    lines = []
    for video in items:
        comments_disabled = False
        ratings_disabled = False
        video_error_or_removed = False
        if "statistics" not in video:
            continue

        video_id = prepare_feature(video['id'])
        snippet = video['snippet']
        statistics = video['statistics']
        features = [prepare_feature(snippet.get(feature, "")) for feature in snippet_features]
        channel_title = snippet.get("channelTitle", "")
        description = snippet.get("description", "")
        category_id = snippet.get("categoryId", "")
        publish_time = snippet.get("publishedAt", "") 
        thumbnail_link = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        trending_date = time.strftime("%y.%d.%m")
        tags = get_tags(snippet.get("tags", ["[none]"]))
        view_count = statistics.get("viewCount", 0)

        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0

        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0

        line = [video_id] +[prepare_feature(trending_date)]+ features + [prepare_feature(x) for x in [channel_title, category_id, publish_time, tags, view_count, likes, dislikes,
                                                                     comment_count, thumbnail_link, comments_disabled,
                                                                     ratings_disabled,video_error_or_removed, description]]

        lines.append(",".join(line))
    return lines


def get_pages(country_code, api_key, next_page_token="&"):
    country_data = []
    filtered_json_data = []
    while next_page_token is not None:
        video_data_page = api_request(next_page_token, country_code, api_key)
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"&pageToken={next_page_token}&" if next_page_token is not None else next_page_token
        items = video_data_page.get('items', [])
        country_data += get_videos(items)
        
        kind = video_data_page.get("kind", "")
        etag = video_data_page.get("etag", "")
        
        for item in items:
            filtered_item = {
                "kind": item.get("kind", ""),
                "etag": item.get("etag", ""),
                "id": item.get("snippet", {}).get("categoryId", ""),
                "snippet": {
                    "channelId": item.get("snippet", {}).get("channelId", ""),
                    "title": item.get("snippet", {}).get("title", "")
                }
            }
            filtered_json_data.append(filtered_item)

    return country_data, kind, etag, filtered_json_data  # Return both CSV data and raw JSON data with kind and etag


def upload_to_s3(bucket_name, file_name, content):
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=content)
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to upload {file_name}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


def write_json_to_s3(bucket_name, file_name, json_data):
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(json_data, ensure_ascii=False, indent=2).encode('utf-8')
        )
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Failed to upload {file_name}: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


def lambda_handler(event, context):
    api_key = os.getenv('API_KEY')
    country_codes = os.getenv('COUNTRY_CODES').split(',')
    bucket_name = os.getenv('BUCKET_NAME')
    output_prefix_csv = os.getenv('OUTPUT_PREFIX_CSV', '')
    output_prefix_json = os.getenv('OUTPUT_PREFIX_JSON', '')

    for country_code in country_codes:
        csv_data, kind, etag, filtered_json_data = get_pages(country_code, api_key)
        
        # Upload CSV file
        csv_buffer = StringIO()
        csv_buffer.write("\n".join([",".join(header)] + csv_data))
        
        # Create the region-specific folder path
        region_folder = f"region={country_code}/"
        
        # Combine the paths to create the full folder path
        full_folder_path = os.path.join(output_prefix_csv, region_folder)
        
        csv_file_name = f"{full_folder_path}{country_code}videos.csv"
        upload_to_s3(bucket_name, csv_file_name, csv_buffer.getvalue())

       # csv_file_name = f"{output_prefix}{time.strftime('%y.%d.%m')}_{country_code}_videos.csv"
       # upload_to_s3(bucket_name, csv_file_name, csv_buffer.getvalue())
       
       
        # Wrap the JSON data inside a root object
        wrapped_json_data = {
            "kind": kind,
            "etag": etag,
            "items": filtered_json_data
        }
        
        
        # Upload JSON file
        json_file_name = f"{output_prefix_json}{country_code}_category_id.json"
        write_json_to_s3(bucket_name, json_file_name, wrapped_json_data)
        
        # Upload JSON file
        #json_file_name = f"{output_prefix}{time.strftime('%y.%d.%m')}_{country_code}_videos.json"
        #write_json_to_s3(bucket_name, json_file_name, filtered_json_data)

    return {
        'statusCode': 200,
        'body': json.dumps('Files uploaded successfully!')
    }
