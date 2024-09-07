import pandas as pd

def process_combined_data(combined_data):
    for gender in ['men', 'women']:
        for event, df in combined_data[gender].items():
            # Ensure the 'Wind' column is present in all DataFrames
            if 'Wind' not in df.columns:
                df['Wind'] = pd.NA

            # Add 'Track/Field' column based on the event type
            df['Track/Field'] = df['Event'].apply(lambda x: 'Track' if any(char.isdigit() for char in x) else 'Field')
    
    return combined_data
