import pandas as pd

def ensure_column_order(df):
    """Ensure the DataFrame columns are in the correct order."""
    expected_columns = [
        'Rank', 'Time', 'Name', 'Country', 'DOB', 'Position_in_race', 
        'City', 'Date', 'Legal', 'Note', 'Sex', 'Event', 'All Conditions Rank', 
        'competition_id', 'Track/Field','Wind'
    ]
    
    # Add missing columns with NaN values
    for col in expected_columns:
        if col not in df.columns:
            df[col] = pd.NA
    
    # Reorder columns
    df = df[expected_columns]
    
    return df


def process_combined_data(combined_data):
    for gender in ['men', 'women']:
        for event, df in combined_data[gender].items():
            if df is None or df.empty:
                print(f"No data to process for {gender}'s {event} event.")
                continue  # Skip this event if there's no data

            # Ensure the 'Wind' column is present in all DataFrames
            if 'Wind' not in df.columns:
                df['Wind'] = pd.NA

            # Add 'Track/Field' column based on the event type
            df['Track/Field'] = df['Event'].apply(lambda x: 'Track' if any(char.isdigit() for char in x) else 'Field')

    return combined_data

