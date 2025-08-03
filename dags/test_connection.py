from azure.storage.blob import BlobServiceClient

conn_str = "DefaultEndpointsProtocol=https;AccountName=airflowdockerstorage;AccountKey=Z3vW2IXp3NlKRx3VBeC9TwlCna5En9MYtAZOf2SAVfaQBfm4sgHwT4n24gz2jCv83u7u1OCp3YQq+AStS3WzNw==;EndpointSuffix=core.windows.net"
container = "raw"

client = BlobServiceClient.from_connection_string(conn_str)
container_client = client.get_container_client(container)

print("Listing blobs...")
for blob in container_client.list_blobs():
    print(blob.name)
print("✅ Done! If no output, container is empty.")
