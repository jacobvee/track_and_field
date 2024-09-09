import pandas as pd

def ensure_column_order(df):
    expected_columns = [
        'Rank', 'Time', 'Wind', 'Name', 'Country', 'DOB', 'Position_in_race', 
        'City', 'Date', 'Legal', 'Note', 'Sex', 'Event', 'All Conditions Rank', 
        'competition_id', 'Track/Field'
    ]
    
    # Add missing columns with NaN values
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Reorder columns
    df = df[expected_columns]
    
    return df


def process_combined_data(combined_data):
    cleaned_data = {'men': {}, 'women': {}}
    
    for gender in ['men', 'women']:
        print(f"Processing data for {gender}")
        if gender in combined_data:
            for event, df in combined_data[gender].items():
                if isinstance(df, pd.DataFrame):
                    print(f"Processing event: {event} for {gender} with shape {df.shape}")
                    
                    # Ensure 'Wind' column exists in all DataFrames
                    if 'Wind' not in df.columns:
                        df['Wind'] = 'N/A'  # Default to 'N/A' if missing
                    
                    # Drop rows with missing DOB, assuming 'DOB' is a column
                    if 'DOB' in df.columns:
                        df = df[df['DOB'] != 'N/A']  # Adjust this condition as necessary
                    
                    # If the DataFrame is still valid, save it
                    if not df.empty:
                        cleaned_data[gender][event] = df
                    else:
                        print(f"Skipping empty DataFrame for {event} ({gender})")
                else:
                    print(f"Invalid data for {event}, skipping...")
    
    return cleaned_data
