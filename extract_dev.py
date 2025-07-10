#Data Extraction using Amplitude's Export API
#https://amplitude.com/docs/apis/analytics/export

#Load Libraries
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
import gzip
import zipfile
import shutil
import tempfile

#Load our Dotenv file
load_dotenv()

#Get URL
URL = "https://analytics.eu.amplitude.com/api/2/export"

#Read Dotenv File
api_key = os.getenv("AMP_API_KEY")
secret_key = os.getenv("AMP_SECRET_KEY")


#Define Parameters



start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
start_time = start_time.strftime("%Y%m%dT%H")
end_time = datetime.now().replace(hour=23, minute=0, second=0, microsecond=0) - timedelta(days=1)
end_time = end_time.strftime("%Y%m%dT%H")

params= {
    'start': start_time,
    'end': end_time
}

#Get response
response = requests.get(URL, params=params, auth=(api_key, secret_key))


# JSON data files saved to a zip folder 'data.zip'
if response.status_code == 200:
    data = response.content
    with open('data/data.zip', 'wb') as file:
        file.write(data)
    print("Data Retrieved Successfully")
else: 
    print(f'Error {response.status_code} :  {response.text}')

# Create a temporary directory
temp_dir = tempfile.mkdtemp()

#Create a Local output directory
data_dir = "data"
os.makedirs(data_dir, exist_ok=True)

#Unpack Zip
with zipfile.ZipFile("data/data.zip", "r") as zip_ref: 
    zip_ref.extractall(temp_dir)

#Locate the folders need file path 
day_folder = next( f for f in os.listdir(temp_dir) if f.isdigit())
day_path = os.path.join(temp_dir, day_folder, )

#Extract Gzips and obtain jsons via triple unpack
for root, _, files in os.walk(day_path): 
     for file in files: 
         if file.endswith('.gz'):
             #Gz to final location
             gz_path = os.path.join(root, file)
             json_filename = file[:-3]
             output_path = os.path.join(data_dir, json_filename)
             with gzip.open(gz_path, 'rb') as gz_file, open(output_path, 'wb') as out_file:
                 shutil.copyfileobj(gz_file, out_file)