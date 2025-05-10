import requests
import os
import zipfile


download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

directory = "/Users/kritijanpeev/data-engineering-practice/downloads"

def main():
    #Create directory if it doesnt exist
    os.makedirs(directory, exist_ok=True)

    for url in download_uris:
        try:
            # Download the file
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # Save ZIP file locally
            zip_filename = os.path.basename(url)
            zip_filepath = os.path.join(directory, zip_filename)
            with open(zip_filepath, "wb") as f:
                f.write(response.content)
            print(f"Downloaded: {zip_filename}")

            # Open and extract CSV from ZIP
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                # Filter to only extract CSV files
                csv_files = [f for f in zip_ref.namelist() if f.lower().endswith(".csv")]
                if csv_files:
                    zip_ref.extractall(path=directory, members=csv_files)
                    print(f"Extracted CSV(s): {', '.join(csv_files)}")
                else:
                    print(f"No CSV found in {zip_filename}")

            # Delete ZIP file after extraction
            os.remove(zip_filepath)
            print(f"Deleted ZIP: {zip_filename}")

        except requests.exceptions.RequestException as e:
            print(f"Failed to download from {url}: {e}")
        except zipfile.BadZipFile:
            print(f"Invalid ZIP file: {zip_filename}")
        except Exception as e:
            print(f"Unexpected error for {url}: {e}")

    pass


if __name__ == "__main__":
    main()
