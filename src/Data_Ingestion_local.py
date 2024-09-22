import requests
import time
import os
#import boto3
import json
from io import StringIO


api_key = 'AIzaSyBpYH5_VhXVtZBwCr7bQqlut20QkeDzGfI'
country_code = 'CA'
page_token="&"

def api_request(page_token, country_code, api_key):
    # Builds the URL and requests the JSON from it
    request_url = (f"https://www.googleapis.com/youtube/v3/videos?part=id,statistics,snippet{page_token}"
                   f"chart=mostPopular&regionCode={country_code}&maxResults=50&key={api_key}")
    request = requests.get(request_url)
    if request.status_code == 429:
        print("Temp-Banned due to excess requests, please wait and continue later")
        raise Exception("Rate limit exceeded")
    return request.json()

response = api_request(page_token, country_code, api_key)

file_path = r'C:\Users\srias\Documents\Github_Projects\YouTube-Content-Insights\response.json'

# Save response as a JSON file
def save_as_json(response, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(response, file, ensure_ascii=False, indent=4)
    print(f"Response saved to {file_path}")

# Save response to a file
save_as_json(response, 'response.json')