import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime


base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
target = datetime.strptime("2024-01-19 10:27", "%Y-%m-%d %H:%M")


def main():
    # Fetch the page
    response = requests.get(base_url)

    # Parse HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    matched_file = None
    for row in soup.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) >= 2:
            link = cols[0].find('a')
            timestamp_text = cols[1].text.strip()

            if link and timestamp_text:
                try:
                    row_time = datetime.strptime(timestamp_text, "%Y-%m-%d %H:%M")
                    if row_time == target:
                        matched_file = link['href']
                        break
                except ValueError:
                    continue

    # Step 3: Download and save the file
    if matched_file:
        file_url = base_url + matched_file
        print(f"Downloading from: {file_url}")

        file_response = requests.get(file_url)
        file_response.raise_for_status()  # Validate the download

        with open(matched_file, 'wb') as f:
            f.write(file_response.content)
        print(f"File saved as: {matched_file}")
    else:
        print("No file matched the exact timestamp.")

    # Step 4: Read with pandas
    df = pd.read_csv(matched_file)

    print(df.head())
    # Clean and convert the temperature column
    df["HourlyDryBulbTemperature"] = pd.to_numeric(df["HourlyDryBulbTemperature"], errors='coerce')

    # Step 5: Find rows with max temperature
    max_temp = df["HourlyDryBulbTemperature"].max()
    max_rows = df[df["HourlyDryBulbTemperature"] == max_temp]

    print(f"\nMax HourlyDryBulbTemperature: {max_temp}")
    print(max_rows)

if __name__ == "__main__":
    main()
