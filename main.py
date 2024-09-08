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

def clear_google_sheet():
    """Clear all the data in the Google Sheet before re-uploading."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    
    if credentials_path:
        print(f"Google credentials found at: {credentials_path}")
        try:
            credentials = service_account.Credentials.from_service_account_file(credentials_path)
        except Exception as e:
            print(f"Error loading credentials: {e}")
            raise
    else:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS environment variable is not set.")
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = credentials.with_scopes(SCOPES)
    
    try:
        service = build('sheets', 'v4', credentials=credentials)
        request_body = {}
        
        # Clear the sheet
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, body=request_body
        ).execute()
        print("Sheet cleared successfully.")
        
    except Exception as e:
        print(f"Failed to clear the Google Sheet: {e}")


def update_google_sheets_in_batches(data_to_update, batch_size=10000, column_headers=None):
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

        # Add column headers first if provided
        if column_headers:
            print("Adding column headers to the sheet...")
            header_range = f"{RANGE_NAME}"
            body = {'values': [column_headers]}
            service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=header_range,
                valueInputOption='RAW',
                body=body
            ).execute()
            print(f"Column headers added: {column_headers}")

        # Iterate over batches and upload in parts
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_data = data_to_update[start:end]
            print(f"Updating rows {start + 1} to {end}...")

            # Update range dynamically based on the batch
            range_start = start + 2  # Add 2 to account for the header row
            range_end = range_start + len(batch_data) - 1
            range_to_update = f"Sheet1!A{range_start}:Z{range_end}"  # Adjust this based on your sheet range
            print(f"Updating range: {range_to_update}")

            body = {'values': batch_data}
            result = service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_to_update,  # Adjust range dynamically based on batch
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

    # Extract column headers
    column_headers = list(df.columns)

    # Update Google Sheets in batches, and pass the column headers
    print(f"Updating Google Sheets for {gender} - {event} in batches...")
    update_google_sheets_in_batches(data_to_update, column_headers=column_headers)


def main():
    # Clear the Google Sheet before updating
    clear_google_sheet()

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
