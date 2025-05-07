import pandas as pd
import requests
import logging
import pyarrow
import time  # Add this import for handling delays
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------------
# 1. FETCH AZURE PRICELIST API DATA
# -------------------------------

def fetch_page(url, max_retries=5, backoff_factor=2):
    """Fetch a single page of Azure Retail Prices API data with retry logic."""
    retries = 0
    while retries < max_retries:
        try:
            logging.info(f"Fetching data from: {url}")
            resp = requests.get(url)
            if resp.status_code == 200:
                return resp.json()
            else:
                logging.warning(f"Failed to fetch data: {resp.status_code} - {resp.text}")
                if resp.status_code == 429:  # Too Many Requests (throttling)
                    logging.warning("API throttling detected. Retrying...")
                else:
                    raise Exception(f"API request failed with status code {resp.status_code}")
        except Exception as e:
            retries += 1
            wait_time = backoff_factor ** retries
            logging.error(f"Error fetching page: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    # If all retries fail, raise an exception
    raise Exception(f"Failed to fetch data from {url} after {max_retries} retries.")

def fetch_azure_prices(api_url):
    """Fetch Azure Retail Prices API data in parallel."""
    prices = []
    next_urls = [api_url]

    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        while next_urls:
            futures = {executor.submit(fetch_page, url): url for url in next_urls}
            next_urls = []
            for future in as_completed(futures):
                try:
                    data = future.result()
                    prices.extend(data['Items'])
                    if 'NextPageLink' in data and data['NextPageLink']:
                        next_urls.append(data['NextPageLink'])
                except Exception as e:
                    logging.error(f"Error fetching page: {e}")
    return prices

# -------------------------------
# 2. EXPAND SAVINGS PLAN DATA
# -------------------------------

def expand_savings_plan(df):
    """Expand savingsPlan column into new rows with updated term and retailPrice."""
    if 'savingsPlan' not in df.columns:
        return df

    # List to store expanded rows
    expanded_rows = []

    # Iterate through each row in the DataFrame
    for _, row in df.iterrows():
        # If savingsPlan exists and is a list, expand it
        if isinstance(row.get('savingsPlan'), list) and len(row['savingsPlan']) > 0:
            for plan in row['savingsPlan']:
                # Create a copy of the row and update term and retailPrice
                new_row = row.copy()
                new_row['reservationTerm'] = plan.get('term', None)
                new_row['retailPrice'] = plan.get('retailPrice', None)
                new_row['type'] = "SavingsPlan"
                expanded_rows.append(new_row)
        else:
            # If no savingsPlan, keep the original row
            expanded_rows.append(row)

    # Create a new DataFrame from the expanded rows
    expanded_df = pd.DataFrame(expanded_rows)

    # Drop the savingsPlan column as it's no longer needed
    #if 'savingsPlan' in expanded_df.columns:
    #    expanded_df = expanded_df.drop(columns=['savingsPlan'])

    return expanded_df

# -------------------------------
# 3. PROCESS ADDITIONAL COLUMNS
# -------------------------------

def process_additional_columns(df_prices):
    """Process additional columns like Spot, Low Priority, and rename reservationTerm."""
    
    # Update type column to "Low Priority" and "Spot" if it's in skuName
    if 'skuName' in df_prices.columns:
        df_prices.loc[df_prices['skuName'].str.contains('Spot', case=False, na=False), 'type'] = 'Spot'
        df_prices.loc[df_prices['skuName'].str.contains('Low Priority', case=False, na=False), 'type'] = 'Low Priority'
    
    # Rename reservationTerm to Term if it exists
    if 'reservationTerm' in df_prices.columns:
        df_prices.rename(columns={'reservationTerm': 'Term'}, inplace=True)
    
    return df_prices

# -------------------------------
# 4. CALCULATE HOURLY PRICE
# -------------------------------

def calculate_hourly_price(df_prices):
    """Add a column HourlyPrice based on type and Term."""
    def calculate_price(row):
        if row['type'] == 'Reservation' and pd.notna(row['Term']):
            term_str = str(row['Term'])
            if '3' in term_str:
                return row['retailPrice'] / (365 * 3 * 24)
            elif '1' in term_str:
                return row['retailPrice'] / (365 * 24)
            elif '5' in term_str:
                return row['retailPrice'] / (5 * 365 * 24)
        return row['retailPrice']  # For non-Reservation types, use retailPrice directly

    # Apply the calculation to each row
    df_prices['HourlyPrice'] = df_prices.apply(calculate_price, axis=1)
    return df_prices

# -------------------------------
# 5. GET AZURE PRICES
# -------------------------------

def get_azure_prices(api_url):
    """Fetch and process Azure Retail Prices API data."""
    logging.info("Fetching Azure Retail Prices API...")
    prices = fetch_azure_prices(api_url)
    logging.info(f"Fetched {len(prices)} price records.")
    df_prices = pd.DataFrame(prices)
    logging.info("Extracting the savings plans...")
    df_prices = expand_savings_plan(df_prices)
    logging.info("Done with savings plans...")
    logging.info("Processing additional columns...")
    df_prices = process_additional_columns(df_prices)
    df_prices = calculate_hourly_price(df_prices)
    return df_prices

# -------------------------------
# 6. EXPORT DISTINCT DATA
# -------------------------------

def export_distinct_data(df_prices, output_distinct_csv_SkuName, output_distinct_csv_LocationsAndArmRegions):
    """Export distinct data for armRegionName, location, and skuName."""
    try:
        # Export distinct armRegionName and location
        df_distinct_locations = df_prices[['armRegionName', 'location']].drop_duplicates()
        df_distinct_locations.to_csv(output_distinct_csv_LocationsAndArmRegions, index=False)
        logging.info(f"Saved distinct Regions list to {output_distinct_csv_LocationsAndArmRegions}")
        
        # Export distinct skuName where productName equals "Virtual Machines"
        df_distinct_sku = df_prices[
            (df_prices['serviceName'] == 'Virtual Machines') & 
            (~df_prices['armSkuName'].str.contains('Type', case=False, na=False))
        ][['skuName', 'armSkuName']].drop_duplicates()
        df_distinct_sku.to_csv(output_distinct_csv_SkuName, index=False)
        
        logging.info(f"Saved distinct skuName list to {output_distinct_csv_SkuName}")
    except Exception as e:
        logging.error(f"An error occurred while exporting distinct data: {e}")
        raise

# -------------------------------
# MAIN
# -------------------------------

def main():
    api_url = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview"
    
    output_csv_path = 'c:/temp/azure_prices.csv'
    output_csv_raw = 'c:/temp/azure_raw_prices.csv'
    output_parquet_path = 'c:/temp/azure_prices.parquet'
    output_distinct_csv_SkuName = 'c:/temp/azure_vm_ArmSkuNames.csv'
    output_distinct_csv_LocationsAndArmRegions = 'c:/temp/azure_vm_LocationsAndArmRegions.csv'
    
    try:
        # Fetch and process Azure prices
        df_prices = get_azure_prices(api_url)
        
        # Save as raw CSV
        df_prices.to_csv(output_csv_raw, index=False)
        logging.info(f"Saved Azure price list data to {output_csv_raw}")
        
        # Save as expanded CSV
        df_prices.to_csv(output_csv_path, index=False)
        logging.info(f"Saved Azure price list data to {output_csv_path}")
        
        # Save as Parquet
        df_prices.to_parquet(output_parquet_path, index=False, engine='pyarrow')
        logging.info(f"Saved Azure price list data to {output_parquet_path}")
        
        # Export distinct data
        export_distinct_data(df_prices, output_distinct_csv_SkuName, output_distinct_csv_LocationsAndArmRegions)
        
        logging.info("Azure price list processing completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()