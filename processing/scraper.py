import pandas as pd
import numpy as np
import hashlib

class AthleticsDataScraper:
    def __init__(self, gender):
        self.base_url = 'https://www.alltime-athletics.com/'
        self.gender = gender

    def fetch_data(self, event, is_legal):
        url = self.generate_url(event, is_legal)
        response = requests.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        pre_tag = soup.find('pre')
        table_text = pre_tag.get_text()
        rows = table_text.split('\n')

        def process_row(row):
            parts = re.split(r'\s{2,}', row)
            return [part.strip() for part in parts]

        data = []
        max_length = 0
        for row in rows:
            if row.strip():
                processed_row = process_row(row)
                data.append(processed_row)
                max_length = max(max_length, len(processed_row))

        if max_length == 10:
            column_names = ["Test", "Rank", "Time", "Wind", "Name", "Country", "DOB", "Position_in_race", "City", "Date"]
        else:
            column_names = ["Test", "Rank", "Time", "Name", "Country", "DOB", "Position_in_race", "City", "Date"]

        df = pd.DataFrame(data, columns=column_names[:max_length])
        df.drop('Test', inplace=True, axis=1, errors='ignore')
        df['Legal'] = 'Y' if is_legal else 'N'

        # Ensure DOB and Date are in a consistent format and handle errors gracefully
        df['Date'] = pd.to_datetime(df['Date'], format='%d.%m.%Y', errors='coerce')
        df['DOB'] = pd.to_datetime(df['DOB'], format='%d.%m.%Y', errors='coerce')  # Fix the date format

        return df

    def add_age_at_time_of_race(self, df):
        # Existing code...
        # Ensure DOB is handled properly during age calculation
        df['Age at Time of Race'] = df.apply(lambda row: row['Date'].year - row['DOB'].year - 
                                             ((row['Date'].month, row['Date'].day) < 
                                              (row['DOB'].month, row['DOB'].day)) 
                                             if pd.notnull(row['DOB']) else pd.NA, axis=1)

        return df

    def get_combined_data(self, event):
        df_legal, has_wind = self.fetch_data(event, True)

        if has_wind:
            df_illegal, _ = self.fetch_data(event, False)
            df_combined = pd.concat([df_legal, df_illegal], ignore_index=True)
        else:
            df_combined = df_legal

        # Drop rows with critical missing data
        df_combined.dropna(subset=['DOB', 'Date'], inplace=True)

        # Ensure correct handling of DOB
        df_combined = self.fill_mode_dob(df_combined)
        df_combined = self.add_age_at_time_of_race(df_combined)
        df_combined = self.add_competition_id(df_combined)

        return df_combined
