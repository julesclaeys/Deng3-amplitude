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

#Set up Parameters
local_folder = "data/"
bucket_folder = 'python-import/'


#List all file names in the list
data_files = os.listdir(local_folder)



#Loop 
for file_name in data_files: 
    if file_name.endswith(".json"):
        #file to upload
        local_file = local_folder + file_name
        #destination set up
        destination = bucket_folder + file_name
        #upload line
        s3_client.upload_file(local_file, bucket, destination)
