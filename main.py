import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from processing.scraper import AthleticsDataScraper
from processing.create_data_frames import run_all_events
from processing.format_data_frames import process_combined_data

# Google Sheets settings
SPREADSHEET_ID = '1MPHHM-L2jcldWxFc2a8-_5bAe9ZhAdmwwr4MKcQcut8'  # Your Google Sheets file ID
RANGE_NAME = 'Sheet1!A1'  # Sheet and starting cell to update

def main():
    # Scrape and clean data
    print("Running event scraper...")
    combined_df = run_all_events()
    print("Combined data fetched from scraper:", combined_df)  # Debug print
    
    print("Processing combined data...")
    combined_data_cleaned = process_combined_data(combined_data=combined_df)
    print("Processed combined data:", combined_data_cleaned)  # Debug print

    # Iterate over 'men' and 'women' keys in combined_data_cleaned dictionary
    for gender, df in combined_data_cleaned.items():
        print(f"Processing data for {gender}...")
        
        # Ensure df is a DataFrame and not empty
        if isinstance(df, pd.DataFrame) and not df.empty:
            # Convert the cleaned dataframe to a list of lists
            data_to_update = df.values.tolist()

            # Prepare Google Sheets update
            print(f"Updating Google Sheets for {gender}...")
            update_google_sheets(data_to_update)
        else:
            print(f"No data available for {gender}.")

def update_google_sheets(data_to_update):
    # Retrieve Google credentials from the environment variable
    print("Attempting to retrieve Google Application credentials...")
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_path:
        print(f"Google credentials found at: {credentials_path}")
        # Load credentials from the credentials file
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            print("Successfully loaded credentials.")
        except Exception as e:
            print(f"Error loading credentials: {e}")
            raise
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Define the Google Sheets API scope
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    print(f"Setting API scopes to: {SCOPES}")
    credentials = credentials.with_scopes(SCOPES)

    # Create the Google Sheets service
    try:
        print("Building Google Sheets service...")
        service = build('sheets', 'v4', credentials=credentials)
        print("Successfully built Google Sheets service.")
    except Exception as e:
        print(f"Failed to build Google Sheets service: {e}")
        raise

    # Prepare data to update the Google Sheets
    print("Preparing data to update Google Sheets...")
    body = {
        'values': data_to_update
    }

    # Update the Google Sheets file
    try:
        print("Updating Google Sheets...")
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
            valueInputOption='RAW', body=body).execute()
        print(f'{result.get("updatedCells")} cells updated.')
    except Exception as e:
        print(f"Failed to update Google Sheets: {e}")


if __name__ == '__main__':
    main()
