import pandas as pd
import numpy as np
import os
import glob
import logging
from sqlalchemy import create_engine # <-- IMPORT THIS

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

INPUT_DATA_PATH = './data/input/'
PROCESSED_DATA_PATH = './data/processed/'
DB_FILE_PATH = 'sales_data.db' # <-- ADD: Path for your SQLite database file

os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)

# --- DATABASE SETUP (NEW) ---
# Create a database engine. This will create the 'sales_data.db' file in your project folder.
db_engine = create_engine(f'sqlite:///{DB_FILE_PATH}')

def find_csv_files(directory_path):
    """Find all CSV files in the specified directory."""
    pattern = os.path.join(directory_path, '*.csv')
    return glob.glob(pattern)

def load_data(file_paths):
    """Load and concatenate data from CSV files, handling different encodings."""
    data_frames = []
    for file in file_paths:
        try:
            df = pd.read_csv(file, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(file, encoding='latin1')
        df['source_file'] = os.path.basename(file)
        data_frames.append(df)
    if data_frames:
        return pd.concat(data_frames, ignore_index=True)
    return pd.DataFrame()

def transform_data(df):
    """Transform and clean the sales data."""
    if df.empty:
        logging.warning("DataFrame is empty. Skipping transformation.")
        return df

    column_map = {
        'ORDERDATE': 'SaleDate', 'PRODUCTCODE': 'Item',
        'QUANTITYORDERED': 'Quantity', 'PRICEEACH': 'Price',
        'PRODUCTLINE': 'Category'
    }

    df = df.rename(columns=column_map)
    df['SaleDate'] = pd.to_datetime(df.get('SaleDate'), errors='coerce')
    df['Quantity'] = pd.to_numeric(df.get('Quantity'), errors='coerce')
    df['Price'] = pd.to_numeric(df.get('Price'), errors='coerce')
    df['Category'] = df.get('Category', 'Unknown').astype(str).fillna('Unknown')
    
    df = df[df['STATUS'].isin(['Shipped', 'Resolved'])]
    df.dropna(subset=['SaleDate', 'Item', 'Quantity', 'Price'], inplace=True)

    if df.empty:
        logging.warning("No valid data after cleaning.")
        return df

    df['TotalSales'] = df['Quantity'] * df['Price']

    # Keep relevant columns for the database
    final_cols = ['SaleDate', 'Item', 'Quantity', 'Price', 'Category', 'TotalSales', 'source_file']
    return df[[col for col in final_cols if col in df.columns]]

# --- NEW FUNCTION TO LOAD DATA TO DATABASE ---
def load_to_database(df, table_name, engine):
    """Loads a DataFrame into a specified table in the database."""
    if df.empty:
        logging.warning("Transformed data is empty, skipping database load.")
        return
    
    try:
        # df.to_sql() is a powerful Pandas function for this
        df.to_sql(
            table_name,
            con=engine,
            if_exists='replace', # 'replace' drops the table first, 'append' adds to it
            index=False         # Don't write the DataFrame index as a column
        )
        logging.info(f"Successfully loaded {len(df)} rows into '{table_name}' table.")
    except Exception as e:
        logging.error(f"Failed to load data to database: {e}")

def main():
    """Main ETL orchestration function."""
    logging.info("Starting ETL process.")
    
    csv_files = find_csv_files(INPUT_DATA_PATH)
    if not csv_files:
        logging.warning("No CSV files found.")
        return

    raw_data = load_data(csv_files)
    transformed_data = transform_data(raw_data)
    
    # --- ADD THE DATABASE LOADING STEP ---
    load_to_database(transformed_data, 'sales', db_engine)
    
    # You can still save the CSV if you want
    if not transformed_data.empty:
        output_file = os.path.join(PROCESSED_DATA_PATH, 'processed_sales_data.csv')
        transformed_data.to_csv(output_file, index=False)
        logging.info(f"Successfully saved transformed data to {output_file}")

if __name__ == '__main__':
    main()