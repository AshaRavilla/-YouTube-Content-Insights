import requests
import os
import boto3
import json
from io import StringIO
import time

# Define headers for CSV output
header = ["video_id", "trending_date", "title", "channelTitle", "categoryId",
          "publish_time", "tags", "views", "likes", "dislikes",
          "comment_count", "thumbnail_link", "comments_disabled",
          "ratings_disabled", "video_error_or_removed", "description"]

# Initialize S3 client
s3_client = boto3.client('s3')

def prepare_feature(feature):
    """Clean up feature text for CSV output."""
    unsafe_characters = ['\n', '"']
    for ch in unsafe_characters:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'

def api_request(page_token, country_code, api_key):
    """Make an API request to fetch video data from YouTube."""
    request_url = (f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet&{page_token}"
                   f"chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}")
    response = requests.get(request_url)
    if response.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        raise Exception("Rate limit exceeded")
    return response.json()

def get_videos(items):
    """Extract video data and format for CSV."""
    lines = []
    for video in items:
        snippet = video['snippet']
        statistics = video['statistics']
        line = [
            prepare_feature(video['id']),
            prepare_feature(time.strftime("%Y-%m-%d")),
            prepare_feature(snippet.get("title", "")),
            prepare_feature(snippet.get("channelTitle", "")),
            prepare_feature(snippet.get("categoryId", "")),
            prepare_feature(snippet.get("publishedAt", "")),
            prepare_feature("|".join(snippet.get("tags", ["[none]"]))),
            prepare_feature(statistics.get("viewCount", "0")),
            prepare_feature(statistics.get("likeCount", "0")),
            prepare_feature(statistics.get("dislikeCount", "0")),
            prepare_feature(statistics.get("commentCount", "0")),
            prepare_feature(snippet["thumbnails"]["default"]["url"]),
            prepare_feature("true" if "commentsDisabled" in snippet else "false"),
            prepare_feature("true" if "ratingsDisabled" in snippet else "false"),
            prepare_feature("false"),  # Assuming no video errors or removed
            prepare_feature(snippet.get("description", ""))
        ]
        lines.append(",".join(line))
    return lines

def get_video_category_details(api_key, region_code):
    """Fetch category IDs and titles from YouTube, including additional details."""
    url = f"https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={region_code}&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to retrieve categories. Status code: {response.status_code}")
        return {}

def upload_to_s3(bucket_name, file_name, content):
    """Upload data to S3."""
    try:
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=content)
        print(f"Successfully uploaded {file_name} to {bucket_name}")
    except Exception as e:
        print(f"Failed to upload {file_name} to {bucket_name}: {e}")
        raise

def lambda_handler(event, context):
    api_key = os.getenv('API_KEY')
    country_codes = os.getenv('COUNTRY_CODES').split(',')
    region_code = os.getenv('REGION_CODE', 'US')
    bucket_name = os.getenv('BUCKET_NAME')
    output_prefix_csv = os.getenv('OUTPUT_PREFIX_CSV', '')
    output_prefix_json = os.getenv('OUTPUT_PREFIX_JSON', '')

    for country_code in country_codes:
        categories_response = get_video_category_details(api_key, region_code)
        video_data_page = api_request("", country_code, api_key)
        csv_data = get_videos(video_data_page.get('items', []))

        # Upload CSV file
        csv_buffer = StringIO()
        csv_buffer.write("\n".join([",".join(header)] + csv_data))
        
        # Create the region-specific folder path
        region_folder = f"region={country_code}/"
        
        # Combine the paths to create the full folder path
        full_folder_path = os.path.join(output_prefix_csv, region_folder)
        
        csv_file_name = f"{full_folder_path}{country_code}_videos.csv"
        upload_to_s3(bucket_name, csv_file_name, csv_buffer.getvalue())

        # Prepare and upload JSON data for category details
        json_file_name = f"{output_prefix_json}{country_code}_category_details.json"
        upload_to_s3(bucket_name, json_file_name, json.dumps(categories_response, indent=2))

    return {
        'statusCode': 200,
        'body': json.dumps('Files uploaded successfully!')
    }

