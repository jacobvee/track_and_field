import pandas as pd

def ensure_column_order(df):
    """Ensure the DataFrame columns are in the correct order."""
    expected_columns = [
        'Rank', 'Time', 'Wind', 'Name', 'Country', 'Position_in_race', 
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
    for gender in ['men', 'women']:
        for event, df in combined_data[gender].items():
            # Ensure the 'Wind' column is present in all DataFrames
            if 'Wind' not in df.columns:
                df['Wind'] = pd.NA

            # Add 'Track/Field' column based on the event type
            df['Track/Field'] = df['Event'].apply(lambda x: 'Track' if any(char.isdigit() for char in x) else 'Field')

            # Ensure consistent column order
            df = ensure_column_order(df)

    return combined_data

