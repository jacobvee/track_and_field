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
    
    print("Processing combined data...")
    combined_data_cleaned = process_combined_data(combined_data=combined_df)

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
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_path:
        # Load credentials from the credentials file
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    # Define the Google Sheets API scope
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(SCOPES)

    # Create the Google Sheets service
    service = build('sheets', 'v4', credentials=credentials)

    # Prepare data to update the Google Sheets
    body = {
        'values': data_to_update
    }

    # Update the Google Sheets file
    try:
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
            valueInputOption='RAW', body=body).execute()
        print(f'{result.get("updatedCells")} cells updated.')
    except Exception as e:
        print(f"Failed to update Google Sheets: {e}")

if __name__ == '__main__':
    main()
