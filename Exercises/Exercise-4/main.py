import os
import json
import pandas as pd


def find_json_files(directory):
    json_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                json_files.append(os.path.join(root, file))
    return json_files


def load_flatten_and_write_json_to_csv(directory, output_dir=None):
    json_files = find_json_files(directory)
    flattened_data = []

    for file_path in json_files:
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                print(data)

                if isinstance(data, list):
                    df = pd.json_normalize(data)
                else:
                    df = pd.json_normalize([data])

                df['__source_file__'] = file_path
                flattened_data.append(df)

            except Exception as e:
                print(f"Error loading {file_path}: {e}")

    if flattened_data:
        combined_df = pd.concat(flattened_data, ignore_index=True)

        # Create output directory if it doesn't exist
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save to a single CSV file
        output_csv_path = os.path.join(output_dir or '.', 'all_data.csv')
        combined_df.to_csv(output_csv_path, index=False)

        print(f"\n✅ All JSON data written to: {output_csv_path}")
    else:
        print("\n⚠️ No valid JSON files found or loaded.")


def main():
    input_directory = 'data'
    output_directory = 'flattened_csv'

    load_flatten_and_write_json_to_csv(input_directory, output_directory)


if __name__ == "__main__":
    main()
