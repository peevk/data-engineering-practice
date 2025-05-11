import boto3
import gzip
import io
from awsume.awsumepy import awsume

def main():
    # Initialize a session using AWS credentials
    s3 = boto3.client('s3', region_name='us-east-1')

    # Define the bucket and key
    bucket_name = 'commoncrawl'
    key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'

    # Specify the local file path where the file will be downloaded
    local_file_path = 'wet.paths.gz'

    # Download the file
    s3.download_file(bucket_name, key, local_file_path)

    print(f"File downloaded to {local_file_path}")

    # Extract and open the .gz file
    with gzip.open(local_file_path, 'rt') as file:
        # Read the first line from the file
        first_line = file.readline().strip()
        print("First line (URI):")
        print(first_line)

        # Prepend the bucket name to the relative URI path
        full_key = first_line  # It's already the path to the object
        print(f"Full S3 path to download: s3://{bucket_name}/{full_key}")

        # Download the file from the full S3 URI
        s3.download_file(bucket_name, full_key, 'downloaded_from_uri.warc.wet.gz')
        print("File downloaded successfully from the URI")

    with gzip.open("downloaded_from_uri.warc.wet.gz", 'rt') as file:
        # Iterate through each line in the file and print it
        for line in file:
            print(line.strip())

if __name__ == "__main__":
    main()
