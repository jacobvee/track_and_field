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
RANGE_NAME = 'Sheet1!A2'  # Sheet and starting cell to update

def update_google_sheets_in_batches(data_to_update, batch_size=10000):
    total_rows = len(data_to_update)
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_path:
        print(f"Google credentials found at: {credentials_path}")
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            print("Successfully loaded credentials.")
        except Exception as e:
            print(f"Error loading credentials: {e}")
            raise
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(SCOPES)

    try:
        service = build('sheets', 'v4', credentials=credentials)

        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_data = data_to_update[start:end]
            print(f"Updating rows {start} to {end}...")

            body = {'values': batch_data}
            result = service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=f'{RANGE_NAME}{start + 1}',  # Adjust range dynamically based on batch
                valueInputOption='RAW',
                body=body
            ).execute()

            print(f"Batch updated {result.get('updatedCells')} cells.")
    except Exception as e:
        print(f"Failed to update Google Sheets: {e}")

def process_and_update(df, gender, event):
    print(f"Processing data for {gender} - {event}...")

    # Handle NaT values
    df = df.fillna("N/A")  # Replace NaT with "N/A" or any placeholder
    data_to_update = df.values.tolist()

    # Update Google Sheets in batches
    print(f"Updating Google Sheets for {gender} - {event} in batches...")
    update_google_sheets_in_batches(data_to_update)

def main():
    # Scrape and clean data
    print("Running event scraper...")
    combined_df = run_all_events()
    print("Combined data fetched from scraper:", combined_df)
    
    print("Processing combined data...")
    combined_data_cleaned = process_combined_data(combined_data=combined_df)
    print("Processed combined data:", combined_data_cleaned)

    # Iterate over 'men' and 'women' keys in combined_data_cleaned dictionary
    for gender, event_data in combined_data_cleaned.items():
        for event, df in event_data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                process_and_update(df, gender, event)
            else:
                print(f"No data available for {gender} - {event}.")

if __name__ == '__main__':
    main()


def update_google_sheets(data_to_update):
    # Check the size of the data to update
    print(f"Updating Google Sheets with {len(data_to_update)} rows.")

    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    if credentials_path:
        print(f"Google credentials found at: {credentials_path}")
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
            print("Successfully loaded credentials.")
        except Exception as e:
            print(f"Error loading credentials: {e}")
            raise
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(SCOPES)

    try:
        service = build('sheets', 'v4', credentials=credentials)
        body = {'values': data_to_update}

        # Send update request
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='RAW',
            body=body
        ).execute()

        print(f'{result.get("updatedCells")} cells updated.')
    except Exception as e:
        print(f"Failed to update Google Sheets: {e}")

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
