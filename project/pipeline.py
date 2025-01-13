import pandas as pd
import zipfile
import io
import requests
import sqlite3
import os
import logging
from datetime import datetime

# Define paths and URL for data download and storage
DATA_DIR = 'data'  # Directory to save processed data
DB_PATH = os.path.join(DATA_DIR, 'jail_deaths.db')  # SQLite database path
ZIP_URL = 'https://graphics.thomsonreuters.com/data/jails/Allstatesinsurvey.zip'  # URL to the zip file containing the dataset
CSV_PATH = os.path.join(DATA_DIR, 'jail_deaths.csv')  # Path to save the cleaned data as a CSV file

# Ensure the data directory exists, creating it if necessary
os.makedirs(DATA_DIR, exist_ok=True)

# Set up logging to monitor the pipeline execution
LOG_FILE = os.path.join(DATA_DIR, 'pipeline_log.txt')
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def download_and_extract_zip(url):
    """
    Download a ZIP file from the given URL, extract its contents, and return a pandas DataFrame
    containing the first Excel file found in the ZIP archive.
    
    Args:
        url (str): The URL from which to download the ZIP file.
        
    Returns:
        pd.DataFrame: A DataFrame containing the data loaded from the Excel file.
    
    Raises:
        FileNotFoundError: If no Excel file is found inside the ZIP.
        Exception: If there's any issue with downloading or extracting the file.
    """
    try:
        logging.info(f"Starting download of dataset from {url}")
        response = requests.get(url)
        response.raise_for_status()  # Ensure we raise an exception for HTTP errors
        logging.info("Dataset downloaded successfully.")

        # Load the ZIP file into memory and list the extracted files
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            extracted_files = zip_ref.namelist()
            logging.info(f"Extracted the following files: {extracted_files}")

            # Find the first Excel file in the ZIP archive
            excel_files = [file for file in extracted_files if file.endswith('.xlsx')]
            if excel_files:
                with zip_ref.open(excel_files[0]) as file:
                    df = pd.read_excel(file)
                logging.info(f"Loaded data from {excel_files[0]} into DataFrame.")
                return df
            else:
                raise FileNotFoundError("No Excel file found in the extracted files.")
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP error during download: {str(e)}")
        raise
    except zipfile.BadZipFile as e:
        logging.error(f"Error extracting ZIP file: {str(e)}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        raise

def load_and_clean_data(df):
    """
    Performs data cleaning on the input DataFrame by handling missing values, removing duplicates,
    and ensuring proper data types for key columns. Logs the data cleaning process.

    Args:
        df (pd.DataFrame): The raw data to be cleaned.

    Returns:
        pd.DataFrame: A cleaned DataFrame.
    """
    logging.info("Commencing data cleaning process.")

    # Remove duplicate rows if they exist
    initial_row_count = len(df)
    df = df.drop_duplicates()
    logging.info(f"Removed {initial_row_count - len(df)} duplicate rows.")

    # Identify and log missing values in key columns
    missing_values = df.isnull().sum()
    logging.info(f"Missing values in the dataset before cleaning:\n{missing_values}")

    # Handling missing values for critical columns:
    if 'cause_of_death' in df.columns:
        df.dropna(subset=['cause_of_death'], inplace=True)
        logging.info("Dropped rows with missing 'cause_of_death'.")

    # Convert the 'date' column to datetime format, handling any errors by coercing invalid dates
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        invalid_dates = df[df['date'].isnull()]
        logging.info(f"Converted 'date' column to datetime. Found {len(invalid_dates)} invalid dates.")
        df.dropna(subset=['date'], inplace=True)

    # Handle missing numerical values (e.g., filling with 0 for 'num_deaths')
    if 'num_deaths' in df.columns:
        df['num_deaths'].fillna(0, inplace=True)
        logging.info("Filled missing 'num_deaths' values with 0.")

    # Drop rows with missing state information (if this is critical for analysis)
    if 'state' in df.columns:
        df.dropna(subset=['state'], inplace=True)
        logging.info("Dropped rows with missing 'state' information.")

    # Final row count after cleaning
    final_row_count = len(df)
    logging.info(f"Data cleaning completed. {initial_row_count - final_row_count} rows removed.")

    return df

def store_data_in_sqlite(df, table_name, db_path=DB_PATH):
    """
    Stores the cleaned DataFrame in an SQLite database.

    Args:
        df (pd.DataFrame): The cleaned data to store.
        table_name (str): The name of the table in the database where the data will be stored.
        db_path (str): Path to the SQLite database file.

    Raises:
        Exception: If there's any issue during data storage.
    """
    try:
        logging.info(f"Storing cleaned data into SQLite database at {db_path} (table: {table_name})")
        conn = sqlite3.connect(db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
        logging.info(f"Data successfully stored in SQLite database.")
    except Exception as e:
        logging.error(f"Error while storing data in SQLite: {str(e)}")
        raise

def store_data_as_csv(df, csv_path=CSV_PATH):
    """
    Saves the cleaned DataFrame to a CSV file.

    Args:
        df (pd.DataFrame): The cleaned data to store.
        csv_path (str): Path where the cleaned data CSV will be saved.

    Raises:
        Exception: If there's any issue during data storage.
    """
    try:
        logging.info(f"Saving cleaned data to CSV at {csv_path}")
        df.to_csv(csv_path, index=False)
        logging.info(f"Data successfully saved to CSV.")
    except Exception as e:
        logging.error(f"Error while saving data to CSV: {str(e)}")
        raise

def main():
    """
    Orchestrates the entire pipeline by downloading, cleaning, and storing the data.
    Logs all major steps in the process for traceability and debugging purposes.
    """
    try:
        # Step 1: Download and extract the dataset from the URL
        df = download_and_extract_zip(ZIP_URL)

        # Step 2: Clean the loaded data
        df = load_and_clean_data(df)

        # Step 3: Save the cleaned data to CSV
        store_data_as_csv(df, CSV_PATH)

        # Optional: Step 4: Store the cleaned data in an SQLite database
        store_data_in_sqlite(df, 'jail_deaths')

    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
