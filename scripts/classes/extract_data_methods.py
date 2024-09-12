import requests
import json
import os
import csv

class JSONFetcher:
    def __init__(self, url: str = None, headers: dict = None):
        """
        Initialize the class with an optional URL and headers.

        :param url: URL of the raw JSON file (can be set later).
        :param headers: Optional custom headers for the HTTP request.
        """
        self.url = url
        self.headers = headers if headers else {}

    def set_url(self, url: str):
        """
        Set or update the URL dynamically.

        :param url: The URL to fetch data from.
        """
        self.url = url

    def set_headers(self, headers: dict):
        """
        Set or update custom headers for the HTTP request.

        :param headers: Dictionary containing HTTP headers.
        """
        self.headers = headers

    def fetch_json(self) -> dict:
        """
        Fetch the JSON data from the provided URL.

        :return: Parsed JSON data as a dictionary if successful, None otherwise.
        """
        if not self.url:
            raise ValueError("No URL has been set. Please provide a URL to fetch data.")
        
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()  # Return the JSON data
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"An error occurred: {err}")
        return None

    def save_json_to_file(self, filename: str, save_directory: str = None) -> None:
        """
        Fetch the JSON data and save it to a local file in the specified directory.

        :param filename: The name of the file to save the JSON data as.
        :param save_directory: Optional directory to save the file in. Defaults to a 'lists' folder.
        """
        if not save_directory:
            current_directory = os.path.dirname(os.path.abspath(__file__))
            save_directory = os.path.join(current_directory, '..', 'lists')

        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, filename)

        data = self.fetch_json()
        if data:
            with open(file_path, 'w') as json_file:
                json.dump(data, json_file, indent=4)
            print(f"Data has been successfully saved to {file_path} in JSON format.")
        else:
            print("Failed to fetch data, file not saved.")

    def save_to_format(self, filename: str, save_directory: str = None, format: str = 'json') -> None:
        """
        Save the fetched content in a different format (e.g., JSON, text, CSV).

        :param filename: The name of the file to save the content as.
        :param save_directory: Optional directory to save the file in.
        :param format: The format in which to save the file ('json', 'text', or 'csv').
        """
        if not save_directory:
            current_directory = os.path.dirname(os.path.abspath(__file__))
            save_directory = os.path.join(current_directory, '..', 'lists')

        os.makedirs(save_directory, exist_ok=True)
        file_path = os.path.join(save_directory, filename)

        data = self.fetch_json()
        if data:
            if format == 'json':
                with open(file_path, 'w') as json_file:
                    json.dump(data, json_file, indent=4)
                print(f"Data has been saved to {file_path} in JSON format.")
            elif format == 'text':
                with open(file_path, 'w') as text_file:
                    text_file.write(json.dumps(data, indent=4))
                print(f"Data has been saved to {file_path} in plain text format.")
            elif format == 'csv':
                self._save_as_csv(file_path, data)
            else:
                print(f"Unsupported format: {format}. Only 'json', 'text', and 'csv' are supported.")
        else:
            print("Failed to fetch data, file not saved.")

    def _save_as_csv(self, file_path: str, data: dict) -> None:
        """
        Save the fetched JSON data as a CSV file.

        :param file_path: The file path to save the CSV.
        :param data: The JSON data to be saved as CSV.
        """
        # Check if data is a list of dictionaries (the most common format for CSV conversion)
        if isinstance(data, list) and isinstance(data[0], dict):
            keys = data[0].keys()
            with open(file_path, 'w', newline='') as csv_file:
                dict_writer = csv.DictWriter(csv_file, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            print(f"Data has been saved to {file_path} in CSV format.")
        else:
            print("Data is not in a format that can be converted to CSV.")

# Example usage
if __name__ == "__main__":
    # URL of the API endpoint
    url = 'https://sentry.lcd.injective.network/injective/exchange/v1beta1/derivative/markets'

    # Create an instance of the JSONFetcher class
    fetcher = JSONFetcher(url)

    # Save the JSON data to a 'lists' folder in the parent directory
    fetcher.save_json_to_file('injective_mainnet_derivative_market.json')

    # Save the file in CSV format
    fetcher.save_to_format('injective_data.csv', format='csv')
