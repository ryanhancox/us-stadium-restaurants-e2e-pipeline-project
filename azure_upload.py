import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError

_ = load_dotenv()

STORAGE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_ID = os.getenv("CONTAINER_ID")

# Create container reference
blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
blob_service_client.get_container_client(CONTAINER_ID)

overwrite = False
target_directory = os.path.join(os.getcwd(), 'Category')
for folder in os.walk(target_directory):
    for file in folder[-1]:
        try:
            blob_path = os.path.join(folder[0].replace(f"{os.getcwd()}\Category\\", ''), file)
            blob_obj = blob_service_client.get_blob_client(container=CONTAINER_ID, blob=blob_path)
            
            with open(os.path.join(folder[0], file), mode='rb') as file_data:
                blob_obj.upload_blob(file_data, overwrite=overwrite)
            
        except ResourceExistsError: # for when we try to upload a file that already exists in a container
            print(f"Blob {blob_path} already exists.")
            print()
            continue