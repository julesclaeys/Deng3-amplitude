#Function to find dates/times with data missing in S3
#Not implemented yet

#Import
import os
import boto3
from dotenv import load_dotenv
from listing_s3_files import list_s3_files
from datetime import datetime, timedelta

load_dotenv()

s3_folder = 'python-import'
bucket_name = os.getenv('AWS_BUCKET_NAME')
aws_access_key_id = os.getenv('AWS_ACCES_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')

start_date = (datetime.today() - timedelta(7)).date()
end_date = (datetime.today() - timedelta(1)).date()

def find_missing_amplitude_data(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder, start_date=None, end_date=None):
    """
    Check S3 bucket for missing Amplitude data files.

    Args:
        bucket_name (str): S3 bucket name
        aws_access_key_id (str): AWS access key
        aws_secret_access_key (str): AWS secret key
        s3_folder (str): S3 folder prefix
        start_date (date, optional): Start date. Defaults to 1 week before yesterday
        end_date (date, optional): End date. Defaults to yesterday

    Returns:
        list: List of tuples (start_date, end_date) for missing data ranges
    """

    #Set up automatic paramters
    if end_date is None:
        end_date = (datetime.today() - timedelta(1)).date()
    if start_date is None:
        start_date = (datetime.today() - timedelta(7)).date()

    #Setup Client Connection
    s3_client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id, 
    aws_secret_access_key = aws_secret_access_key  
    )

    #Obtain Metadata

    file_list = list_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder="")
    date_time_list = []
    for file in file_list:
        file = file.split('#')[0]
        date_time_list.append(file.split('_', 1)[-1])

    moving_date = start_date
    missing_dates = []

    while moving_date < end_date: 
        moving_date = moving_date + timedelta(1)
        time = 0
        while time < 25:
            check_dt = str(moving_date) + '_' + str(time)
            if check_dt not in date_time_list:
                missing_dates.append(check_dt)
            time += 1
        
    return missing_dates


print(find_missing_amplitude_data(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder, start_date=None, end_date=None))