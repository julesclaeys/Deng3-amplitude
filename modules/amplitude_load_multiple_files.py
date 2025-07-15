# Load libraries
import os
import boto3
from dotenv import load_dotenv
from modules.extract_json_files import extract_json_files

# Load .env file
load_dotenv()

# Read .env file
aws_access_key=os.getenv('AWS_ACCESS_KEY')
aws_secret_key=os.getenv('AWS_SECRET_KEY')
aws_bucket_name=os.getenv('AWS_BUCKET')