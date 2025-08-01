#Data Extraction using Amplitude's Export API
#https://amplitude.com/docs/apis/analytics/export

#Load Libraries
import os
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

def extract_amplitude_data(start_date, end_date, api_key, secret_key, output_file='data/data.zip'):

    """
    This function extracts data from Amplitude's Export API for a given data range and stores it in an output file which is called data.zip by default

    Args:
        Start_time (str): Start date in format 'YYYYMMDDTHH' (e.g., '20241101T00')
        End_time (str): End date in format 'YYYYMMDDTHH' (e.g., '20241101T00')
        api_key (str): Amplitude API key
        secret_key (str): Amplitude Secret 
        output_file (str):  File name to be output, set by default to data.zip


    Output: 
        bool: True if the extraction was successful, False otherwise. 

    """

    #Get URL
    URL = "https://analytics.eu.amplitude.com/api/2/export"

    #Read Dotenv File
    api_key = os.getenv("AMP_API_KEY")
    secret_key = os.getenv("AMP_SECRET_KEY")

    params= {
        'start': start_date,
        'end': end_date
    }

    try: 
                #Get response
        response = requests.get(URL, params=params, auth=(api_key, secret_key))
                # JSON data files saved to a zip folder 'data.zip'
        if response.status_code == 200:
            data = response.content
            with open(output_file, 'wb') as file:
                    file.write(data)
            print(f'Data saved to {output_file}')
            return True
        else: 
            print('f Error {response.status_code}: {response.text}')
            return False
    except Exception as e:
        print(f'An Error has occured!: {str(e)}')
        return False

#This code has to be followed by the unzip.json and then a load module not created yet. 