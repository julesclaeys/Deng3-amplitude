#Listing Files in S3 Bucket Folder using Boto3
#This is an example of module which could be built for better extraction in a longer project
import boto3

def list_s3_files(bucket_name, aws_access_key_id, aws_secret_access_key, s3_folder=""):
    """
    List all files in an S3 bucket folder.
    
    Args:
        bucket_name (str): S3 bucket name
        aws_access_key_id (str): AWS access key
        aws_secret_access_key (str): AWS secret key
        s3_folder (str): Folder path (e.g., "my-folder/")
    
    Returns:
        list: File names in the folder
    """

    #Setup Client Connection
    s3_client = boto3.client(
    's3',
    aws_access_key_id = aws_access_key_id, 
    aws_secret_access_key = aws_secret_access_key  
    )

    #Obtain Metadata
    s3 = s3_client.list_objects_v2(Bucket=bucket_name, Prefix='python-import/')
    #Set up list to return
    file_list = []

    #Loop Through Metadata content and find key equivalent to the file name
    for file in s3['Contents']:
        #obtain file_path
        file_path = (file['Key'])
        #Append only the name of the file
        file_list.append(file_path.split('/')[1])
        
    return file_list