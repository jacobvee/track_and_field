import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from processing.scraper import AthleticsDataScraper, ensure_column_order
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


def update_google_sheets_in_batches(data_to_update, batch_size=10000, start_row=2):
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

        # Iterate over batches and upload in parts
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            batch_data = data_to_update[start:end]
            print(f"Updating rows {start_row} to {start_row + len(batch_data) - 1}...")

            # Update range dynamically based on the batch
            range_to_update = f"Sheet1!A{start_row}:Z{start_row + len(batch_data) - 1}"  # Adjust this based on your sheet range
            print(f"Updating range: {range_to_update}")

            body = {'values': batch_data}
            result = service.spreadsheets().values().update(
                spreadsheetId=SPREADSHEET_ID,
                range=range_to_update,  # Adjust range dynamically based on batch
                valueInputOption='RAW',
                body=body
            ).execute()

            print(f"Batch updated {result.get('updatedCells')} cells.")
            # Move start_row to the next available row for the next batch
            start_row += len(batch_data)

        return start_row  # Return the new starting row for the next update
    except Exception as e:
        print(f"Failed to update Google Sheets: {e}")

def process_and_update(df, gender, event, start_row):
    print(f"Processing data for {gender} - {event}...")

    # Ensure DOB and Date columns are in datetime format before using .dt.strftime
    if 'DOB' in df.columns:
        df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')  # Convert to datetime
        df['DOB'] = df['DOB'].dt.strftime('%Y-%m-%d').fillna("N/A")
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')  # Convert to datetime
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d').fillna("N/A")
    
    # Debugging print to ensure DOB and Date are correct before upload
    print(f"DOB values before sending to Google Sheets for {gender} - {event}:")
    print(df['DOB'].head())
    print(f"Date values before sending to Google Sheets for {gender} - {event}:")
    print(df['Date'].head())

    # Convert DataFrame to list of lists for Google Sheets API
    data_to_update = df.fillna("N/A").values.tolist()

    # Print the number of rows to be uploaded
    print(f"Data size for {gender} - {event}: {len(data_to_update)} rows")

    # Update Google Sheets in batches, passing the updated start_row
    print(f"Updating Google Sheets for {gender} - {event} in batches...")
    new_start_row = update_google_sheets_in_batches(data_to_update, start_row=start_row)
    
    return new_start_row  # Return the new starting row for the next event


def main():
    # Clear the Google Sheet before updating
    clear_google_sheet()

    # Scrape and clean data
    print("Running event scraper...")
    combined_df = run_all_events()
    
    print("Processing combined data...")
    combined_data_cleaned = process_combined_data(combined_data=combined_df)

    # Extract column headers (assuming all DataFrames have the same columns)
    first_event_df = next(iter(next(iter(combined_data_cleaned.values())).values()))
    column_headers = list(first_event_df.columns)

    # Add column headers to the sheet before adding data
    update_google_sheets_in_batches([column_headers], start_row=1)

    # Start uploading the data after the headers (row 2 onwards)
    current_start_row = 2

    # Iterate over 'men' and 'women' keys in combined_data_cleaned dictionary
    for gender, event_data in combined_data_cleaned.items():
        for event, df in event_data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                # Update Google Sheets and get the next starting row
                current_start_row = process_and_update(df, gender, event, current_start_row)
            else:
                print(f"No data available for {gender} - {event}.")

if __name__ == '__main__':
    main()
