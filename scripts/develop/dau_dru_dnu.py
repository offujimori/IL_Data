import sys
import os
import json
import ijson  # For streaming large JSON files
from ijson.common import IncompleteJSONError  # Import the error to catch

def count_dau_returning_new_users_across_files(json_files: list[str], json_files_directory: str) -> list[dict]:
    """
    Calculate the Daily Active Users (DAU), Daily Returning Users (DRU), and New Users across multiple JSON files (by year and month).
    
    Args:
    - json_files: List of filenames of the JSON files to process.
    - json_files_directory: The directory where the JSON files are located.
    
    Returns:
    - List of dictionaries with 'date', 'dau', 'returningAddresses', and 'newUsers' for all files combined.
    """
    # A set to track all subaccountIds that have been seen so far (across all files)
    seen_subaccounts = set()

    # Initialize the result list
    result = []

    # Process each JSON file
    for json_file in json_files:
        json_file_path = os.path.join(json_files_directory, json_file)
        print(f"Processing file: {json_file}")

        try:
            # Stream the JSON data using ijson (to handle large files)
            with open(json_file_path, 'r') as file:
                data_stream = ijson.items(file, 'item')

                # Process each day's data from the current file
                for day_data in data_stream:
                    date = day_data['date']
                    current_subaccounts = set(day_data['subaccountIds'])

                    # Calculate DAU (total active users for the day)
                    dau = len(current_subaccounts)

                    # Find returning users: subaccounts that were active on a previous day
                    returning_addresses = current_subaccounts.intersection(seen_subaccounts)

                    # Calculate new users (DAU - Returning Addresses)
                    new_users = dau - len(returning_addresses)

                    # Update the seen_subaccounts set to include all subaccounts from the current day
                    seen_subaccounts.update(current_subaccounts)

                    # Append the result for the day
                    result.append({
                        'date': date,
                        'dau': dau,  # Total active users on this day
                        'returningAddresses': len(returning_addresses),  # Users who were active on a previous day
                        'newUsers': new_users  # Users who are new (active for the first time)
                    })
        except IncompleteJSONError:
            print(f"Error processing file {json_file}: Incomplete JSON data. Skipping this file.")
            continue

    return result


def main(market_type:str, output_filename:str):
    # Get the current directory of the script (markets.py)
    current_directory = os.path.dirname(os.path.abspath(__file__))

    # Append the path two levels up to sys.path, so we can import from 'classes'
    sys.path.append(os.path.join(current_directory, '..', 'inputs'))

    # Path to the directory containing all JSON files (one for each year and month)
    json_files_directory = os.path.join(current_directory, '..', 'inputs', 'dau_dru_dnu', market_type)

    # List all JSON files in the directory (assuming they are named like exchangeV2.spot_trades_YYYY_M.json)
    json_files = [f for f in os.listdir(json_files_directory) if f.endswith('.json')]

    # Sort the JSON files by year and month (extracting both from the filename)
    json_files.sort(key=lambda x: (int(x.split('_')[-2]), int(x.split('_')[-1].split('.')[0])))

    # Calculate DAU, Returning Users, and New Users data
    dau_returning_new_users_data = count_dau_returning_new_users_across_files(json_files, json_files_directory)

    # Output the results
    for day in dau_returning_new_users_data:
        print(f"Date: {day['date']}, DAU: {day['dau']}, Returning Addresses: {day['returningAddresses']}, New Users: {day['newUsers']}")

    # Export the results to a JSON file
    output_file_path = os.path.join(current_directory, '..', 'outputs', output_filename)
    with open(output_file_path, 'w') as output_file:
        json.dump(dau_returning_new_users_data, output_file, indent=4)

    print(f"Data has been exported to {output_file_path}")

if __name__ == "__main__":
    main('spot', 'spot_dau_returning_new_users_data.json')
    main('derivative', 'derivative_dau_returning_new_users_data.json')
