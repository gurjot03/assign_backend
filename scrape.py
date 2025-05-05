from playwright.sync_api import sync_playwright
import pandas as pd
import time
import re

def initialize_browser():
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    return playwright, browser, page

def get_test_type_full_form(letter):
    test_type_map = {
        'A': 'Ability & Aptitude',
        'B': 'Biodata & Situational Judgement',
        'C': 'Competencies',
        'D': 'Development & 360',
        'E': 'Assessment Exercises',
        'K': 'Knowledge & Skills',
        'P': 'Personality & Behavior',
        'S': 'Simulations'
    }
    return test_type_map.get(letter.upper(), letter)

def scrape_product_details(page, url):
    print(f"Visiting product page: {url}")
    page.goto(url)
    
    page.wait_for_selector('.product-catalogue-training-calendar__row', timeout=10000)
    
    details = {}
    
    try:
        description_div = page.query_selector('.product-catalogue-training-calendar__row:has(h4:text-is("Description"))')
        if description_div:
            description = description_div.query_selector('p').inner_text().strip()
            details['Description'] = description
    except:
        details['Description'] = "Not available"
    
    try:
        job_levels_div = page.query_selector('.product-catalogue-training-calendar__row:has(h4:text-is("Job levels"))')
        if job_levels_div:
            job_levels = job_levels_div.query_selector('p').inner_text().strip()
            details['Job Levels'] = job_levels
    except:
        details['Job Levels'] = "Not available"
    
    try:
        languages_div = page.query_selector('.product-catalogue-training-calendar__row:has(h4:text-is("Languages"))')
        if languages_div:
            languages = languages_div.query_selector('p').inner_text().strip()
            details['Languages'] = languages
    except:
        details['Languages'] = "Not available"
    
    try:
        assessment_div = page.query_selector('.product-catalogue-training-calendar__row:has(h4:text-is("Assessment length"))')
        if assessment_div:
            assessment_text = assessment_div.query_selector('p').inner_text().strip()
            # Extract number using regex
            match = re.search(r'(\d+)', assessment_text)
            if match:
                details['Assessment Length'] = match.group(1)
            else:
                details['Assessment Length'] = assessment_text
    except:
        details['Assessment Length'] = "Not available"
    
    return details

def scrape_shl_product_catalog():
    playwright, browser, page = initialize_browser()
    all_results = []

    for start in range(0, 133, 12):
        url = f"https://www.shl.com/products/product-catalog/?start={start}&type=2"
        print(f"Scraping page with start={start}...")
        
        page.goto(url)
        page.wait_for_selector('table', timeout=10000)
        
        rows = page.query_selector_all('table > tbody > tr:not(:first-child)')
        
        results = []
        for row in rows:
            name_cell = row.query_selector('td.custom__table-heading__title a')
            if name_cell:
                name = name_cell.inner_text().strip()
                url = name_cell.get_attribute('href')
                if url and not url.startswith('http'):
                    url = f"https://www.shl.com{url}"
            else:
                continue 
            
            remote_circle = row.query_selector('td:nth-child(2) span.catalogue__circle')
            remote_testing = 'Yes' if remote_circle and '-yes' in remote_circle.get_attribute('class') else 'No'
            
            adaptive_circle = row.query_selector('td:nth-child(3) span.catalogue__circle')
            adaptive = 'Yes' if adaptive_circle and '-yes' in adaptive_circle.get_attribute('class') else 'No'
            
            test_type_letters = []
            test_type_spans = row.query_selector_all('td:nth-child(4) span.product-catalogue__key')
            for span in test_type_spans:
                letter = span.inner_text().strip()
                full_form = get_test_type_full_form(letter)
                test_type_letters.append(full_form)
            
            product_data ={
                'Name': name,
                'URL': url,
                'Remote Testing': remote_testing,
                'Adaptive/IRT': adaptive,
                'Test Type': ', '.join(test_type_letters)
            }
            results.append(product_data)
            print(f"Completed extracting data for: {name}")
        
        all_results.extend(results)
        time.sleep(2)

    browser.close()
    playwright.stop()
    
    return all_results

def enrich_product_catalog():
    print("Loading existing product catalog...")
    df = pd.read_csv("shl_product_catalog.csv")
    
    playwright, browser, page = initialize_browser()
    
    try:
        for index, row in df.iterrows():
            url = row['URL']
            print(f"Processing {index + 1}/{len(df)}: {row['Name']}")
            
            try:
                details = scrape_product_details(page, url)
                for key, value in details.items():
                    df.at[index, key] = value
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")
            
            if (index + 1) % 10 == 0:
                df.to_csv("shl_product_catalog_enriched.csv", index=False)
                print(f"Progress saved - {index + 1} items processed")
            
            time.sleep(1)
    
    finally:
        browser.close()
        playwright.stop()
    
    df.to_csv("shl_product_catalog_enriched2.csv", index=False)
    print("\nEnrichment complete! Saved to shl_product_catalog_enriched2.csv")


if __name__ == "__main__":
    # print("Starting SHL product catalog scraper...")
    # results = scrape_shl_product_catalog()
    # df = pd.DataFrame(results)
    # df.to_csv("shl_product_catalog.csv")
    # print(f"\nSaved {len(results)} products to shl_product_catalog1.csv")
    enrich_product_catalog()
