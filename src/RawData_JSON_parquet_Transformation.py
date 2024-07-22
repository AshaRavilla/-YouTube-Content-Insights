import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import time

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']
os_input_write_data_operation = os.environ['write_data_operation']

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("bucket =", bucket)
    print("key =", key)
    try:
        start_time = time.time()

        # Creating DF from content
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')
        print("df_raw head:", df_raw.head(5))

        # Extract required columns
        df_step_1 = pd.json_normalize(df_raw['items'])
        print("data is normalized")

        # Log time after normalization
        normalization_time = time.time()
        print("Time taken for normalization:", normalization_time - start_time)

        # Write to S3 with Glue catalog integration
        wr_response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )
        print("wr_response:", wr_response)

        # Log time after writing to S3
        s3_write_time = time.time()
        print("Time taken for writing to S3:", s3_write_time - normalization_time)

        return wr_response
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function.')
        raise e
