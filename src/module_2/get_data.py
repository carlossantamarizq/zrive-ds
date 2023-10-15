import boto3
import os
from dotenv import load_dotenv


load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")



s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

data_folder = 'data'

# Crea el directorio local si no existe
if not os.path.exists(data_folder):
    os.makedirs(data_folder)

bucket_name = "zrive-ds-data"


directories = ["groceries/sampled-datasets/", "groceries/box_builder_dataset/"]

for directory in directories:

    objects = s3.list_objects(Bucket=bucket_name, Prefix=directory)

    for obj in objects.get('Contents'):
        aws_file = obj.get('Key')

        filename = aws_file.split("/")[-1]

        local_file = f"{data_folder}/{filename}"

        print(f"Downloading {filename}...")

        s3.download_file(bucket_name, aws_file, local_file)

        print(f"{filename} downloaded.")
