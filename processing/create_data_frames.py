
from processing.scraper import AthleticsDataScraper

def run_all_events():
    events = [
        '200', '100m', '400', 'long', 'trip', '110h', '400h', 'pole', 
        'shot', 'disc', 'jave', 'hamm', 'deca', '60m', '300', '800', 
        '5000', '10000', '1500'
    ]
    
    results = {'men': {}, 'women': {}}
    
    scraper_male = AthleticsDataScraper(gender='male')
    scraper_female = AthleticsDataScraper(gender='woman')
    
    for event in events:
        print(f"Fetching data for men's {event} event...")
        df_men = scraper_male.get_combined_data(event)
        if df_men is not None and not df_men.empty:
            results['men'][event] = df_men
        else:
            print(f"No valid data for men's {event} event.")

        # Swap '110h' for '100h' and 'deca' for 'hept' for women
        female_event = '100h' if event == '110h' else 'hept' if event == 'deca' else event
        print(f"Fetching data for women's {female_event} event...")
        df_women = scraper_female.get_combined_data(female_event)
        if df_women is not None and not df_women.empty:
            results['women'][female_event] = df_women
        else:
            print(f"No valid data for women's {female_event} event.")
    
    return results

