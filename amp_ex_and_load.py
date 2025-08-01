#Future version of the code with modules
from modules.extract_amplitude_files import extract_amplitude_data
from modules.unzip_json import extract_json_files

import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

#Load our Dotenv file
load_dotenv()

#Read Dotenv File
api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")

#Define Parameters
start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
start_date = start_date.strftime("%Y%m%dT%H")
end_date = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0) - timedelta(days=1)
end_date = end_date.strftime("%Y%m%dT%H")
zip_file_path = "data/data.zip"

#Extract Data.zip file from amplitude
extract_amplitude_data(start_date, end_date, api_key, secret_key)

#Unzips them
extract_json_files(zip_file_path, output_dir="data")