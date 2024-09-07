
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
        results['men'][event] = scraper_male.get_combined_data(event)
        
        # Swap '110h' for '100h' and 'deca' for 'hept' for women
        if event == '110h':
            female_event = '100h'
        elif event == 'deca':
            female_event = 'hept'
        else:
            female_event = event
            
        print(f"Fetching data for women's {female_event} event...")
        results['women'][female_event] = scraper_female.get_combined_data(female_event)
    
    return results

