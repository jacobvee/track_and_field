import os
import json
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from processing.scraper import AthleticsDataScraper
from processing.create_data_frames import run_all_events
from processing.format_data_frames import process_combined_data

def main():
    # Create and clean the dataframe by scraping and processing the data
    print("Running event scraper...")
    combined_df = run_all_events()
    print("Processing combined data...")
    combined_data_cleaned = process_combined_data(combined_data=combined_df)
    
    # Save the dataframe as a CSV file
    csv_file = 'data.csv'
    combined_data_cleaned.to_csv(csv_file, index=False)
    print(f'Data saved as {csv_file}')

    # Retrieve Google credentials from the environment variable
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_path:
        # Load credentials from the credentials file
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Define the Google Drive API scope
    SCOPES = ['https://www.googleapis.com/auth/drive.file']
    credentials = credentials.with_scopes(SCOPES)

    # Create the Google Drive service
    service = build('drive', 'v3', credentials=credentials)

    # Prepare file metadata and media for upload
    file_metadata = {'name': csv_file}
    media = MediaFileUpload(csv_file, mimetype='text/csv')

    # Upload the file to Google Drive
    print("Uploading file to Google Drive...")
    file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    print(f'File uploaded successfully! File ID: {file.get("id")}')

if __name__ == '__main__':
    main()
