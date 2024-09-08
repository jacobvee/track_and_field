import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import numpy as np
import hashlib

def ensure_column_order(df):
    """Ensure the DataFrame columns are in the correct order."""
    expected_columns = [
        'Rank', 'Time', 'Wind', 'Name', 'Country', 'DOB', 'Position_in_race', 
        'City', 'Date', 'Legal', 'Note', 'Sex', 'Event', 'All Conditions Rank', 
        'Age at Time of Race', 'competition_id', 'Track/Field'
    ]
    
    # Add missing columns with NaN values
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Reorder columns
    df = df[expected_columns]
    
    return df

class AthleticsDataScraper:
    def __init__(self, gender):
        self.base_url = 'https://www.alltime-athletics.com/'
        self.gender = gender  # Add gender as a class attribute
    
    def generate_url(self, event, is_legal):
        special_cases_male = {
            '100m': ('m_100ok.htm', 'm100mno.htm'),
            'trip': ('mtripok.htm', 'mtripno.htm'),
            'long': ('mlongok.htm', 'mlongno.htm'),
            '110h': ('m_110hok.htm', 'm_110hno.htm'),
            'pole': ('mpoleok.htm','mpoleno.htm'),
            'shot': ('mshotok.htm','mshotno.htm'),
            'disc': ('mdiscok.htm','mdiscno.htm'),
            'jave': ('mjaveok.htm','mjaveno.htm'),
            'hamm': ('mhammok.htm','mhammno.htm'),
            'deca': ('mdecaok.htm','mdecano.htm'),
            '60m':   ('m60mok.htm','m60mno.htm'),
            '800m':   ('m_800ok.htm','m_800no.htm'),
            '1500m': ('m_1500ok.htm','m_1500no.htm'),
            '5000m':   ('m_5000ok.htm','m_5000no.htm'),
            '10000':   ('m_10kok.htm','10kno.htm')
        }
        special_cases_female = {
            '100m': ('w_100ok.htm', 'w_100no.htm'),
            'trip': ('wtripleok.htm', 'wtripleno.htm'),
            'long': ('wlongok.htm', 'wlongno.htm'),
            '100h': ('w_100hok.htm', 'w_100hno.htm'),
            'pole': ('wpoleok.htm','wpoleno.htm'),
            'shot': ('wshotok.htm','wshotno.htm'),
            'disc': ('wdiscok.htm','wdiscno.htm'),
            'jave': ('wjaveok.htm','wjaveno.htm'),
            'hamm': ('whammok.htm','whammno.htm'),
            'hept': ('whepaok.htm','wheptno.htm'),
            '60m':   ('w60mok.htm','w60mno.htm'),
            '800m':   ('w_800ok.htm','w_800no.htm'),
            '1500m': ('w_1500ok.htm','w_1500no.htm'),
            '5000m':   ('w_5000ok.htm','w_5000no.htm'),
            '10000':   ('w_10kok.htm','w_10kno.htm')
        }
        
        special_cases = special_cases_male if self.gender == 'male' else special_cases_female
        
        if event in special_cases:
            legal_suffix, illegal_suffix = special_cases[event]
            suffix = legal_suffix if is_legal else illegal_suffix
        else:
            suffix = f"{self.gender[0]}_{event}{'ok' if is_legal else 'no'}.htm"
        
        return f"{self.base_url}{suffix}"


    def convert_mmss_to_seconds(self, time_str):
        parts = time_str.split(':')
        if len(parts) == 2:
            minutes = int(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
        else:
            return np.nan
    
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

        # Define column names based on the maximum row length
        if max_length == 10:
            column_names = ["Test", "Rank", "Time", "Wind", "Name", "Country", "DOB", "Position_in_race", "City", "Date"]
        else:
            column_names = ["Test", "Rank", "Time", "Name", "Country", "DOB", "Position_in_race", "City", "Date"]

        df = pd.DataFrame(data, columns=column_names[:max_length])
        df.drop('Test', inplace=True, axis=1, errors='ignore')
        df['Legal'] = 'Y' if is_legal else 'N'
        has_wind = 'Wind' in df.columns
        return df, has_wind
    
    def add_all_conditions_rank(self, df, event):
        if re.search(r'\d', event):
            # This is a race event
            df['All Conditions Rank'] = df['Time'].rank(method='min')
        else:
            # This is a field event
            df['All Conditions Rank'] = df['Time'].rank(ascending=False, method='min')
        return df


    def add_competition_id(self, df):
        # Ensure 'Date' is formatted correctly
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        # Create a concatenated string of Date and City
        df['competition_id'] = df.apply(lambda row: f"{row['Date']}_{row['City']}", axis=1)

        # Hash the concatenated string to create a unique ID
        df['competition_id'] = df['competition_id'].apply(lambda x: hashlib.sha1(x.encode()).hexdigest())

        return df
    
    def fill_mode_dob(self, df):
        # Calculate mode DOB for each athlete (Name)
        mode_dob = df.groupby('Name')['DOB'].agg(lambda x: x.mode().iat[0] if not x.mode().empty else np.nan).reset_index()
        
        # Merge mode DOB back into original DataFrame
        df = df.merge(mode_dob, on='Name', suffixes=('', '_mode'))
        
        # Replace DOB with mode DOB where DOB is missing or different from mode DOB
        df['DOB'] = df.apply(lambda row: row['DOB_mode'] if pd.isnull(row['DOB']) or row['DOB'] != row['DOB_mode'] else row['DOB'], axis=1)
        
        # Drop temporary columns
        df.drop(columns=['DOB_mode'], inplace=True)
        
        return df

    def get_combined_data(self, event):
        df_legal, has_wind = self.fetch_data(event, True)

        if has_wind:
            df_illegal, _ = self.fetch_data(event, False)
            df_combined = pd.concat([df_legal, df_illegal], ignore_index=True)
        else:
            df_combined = df_legal

        # Handle invalid dates by replacing '00' with '01'
        df_combined['Date'] = df_combined['Date'].str.replace(r'00\.00\.', '01.01.', regex=True)
        df_combined['DOB'] = df_combined['DOB'].str.replace(r'00\.00\.', '01.01.', regex=True)

        # Convert the Date and DOB columns to datetime
        df_combined['Date'] = pd.to_datetime(df_combined['Date'], format='%d.%m.%Y', errors='coerce')
        df_combined['DOB'] = pd.to_datetime(df_combined['DOB'], format='%d.%m.%Y', errors='coerce')

        # Fill mode DOB for missing or incorrect dates
        df_combined = self.fill_mode_dob(df_combined)

        # Process the time and note columns
        df_combined['Note'] = df_combined['Time'].str.extract(r'([a-zA-Z#*@+´]+)', expand=False)
        df_combined['Time'] = df_combined['Time'].str.replace(r'[a-zA-Z#*@+´]', '', regex=True)
        if event in ['800', '1500', '5000', '10000']:
            df_combined['Time'] = df_combined['Time'].apply(self.convert_mmss_to_seconds)

        df_combined['Time'] = df_combined['Time'].astype('float')
        df_combined['Sex'] = 'Male' if self.gender == 'male' else 'Female'
        df_combined['Event'] = event

        # Add rankings and competition ID
        df_combined = self.add_all_conditions_rank(df_combined, event)
        df_combined = self.add_competition_id(df_combined)

        return df_combined
