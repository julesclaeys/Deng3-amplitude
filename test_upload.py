# Load libraries
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

bucket = os.getenv('AWS_BUCKET_NAME')
access_key = os.getenv('AWS_ACCES_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')

s3_client = boto3.client(
    's3',
    aws_access_key_id = access_key, 
    aws_secret_access_key = secret_key
)

local_file = "test_file.json"
bucket_folder = "python-import/test_file.json"


s3_client.upload_file(local_file, bucket, bucket_folder)
