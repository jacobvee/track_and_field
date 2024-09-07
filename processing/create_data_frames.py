
from processing.scraper import AthleticsDataScraper

def run_all_events():
    events = [
        '200', '100m', '400', 'long', 'trip', '110h', '400h', 'pole', 
        'shot', 'disc', 'jave', 'hamm', 'deca', '60m', '300', '800', 
        '5000', '10000', '1500'
    ]
    
    results = {'men': {}, 'women': {}}
    
    scraper_male = AthleticsDataScraper(gender='male')
    scraper_female = AthleticsDataScraper(gender='female')
    
    for event in events:
        print(f"Fetching data for men's {event} event...")
        results['men'][event] = scraper_male.get_combined_data(event)
        
        print(f"Fetching data for women's {event} event...")
        results['women'][event] = scraper_female.get_combined_data(event)
    
    return results

