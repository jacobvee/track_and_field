from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import json
import os

from processing.scraper import AthleticsDataScraper
from processing.create_data_frames import run_all_events
from processing.format_data_frames import process_combined_data

combined_df = run_all_events()
combined_data_cleaned  = process_combined_data(combined_data=combined_df)

SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Load credentials from environment variable
credentials_info = json.loads(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)

# Create Google Drive service
service = build('drive', 'v3', credentials=credentials)

# File metadata and media
file_metadata = {'name': 'data.csv'}
media = MediaFileUpload('data.csv', mimetype='text/csv')

# Upload file
file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
print(f'File ID: {file.get("id")}')