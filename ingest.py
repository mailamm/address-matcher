# Import necessary libraries
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Use database URL from environment variable for security
DB_URL = os.getenv("DB_URL")

# Create SQLAlchemy engine using the database URL
engine = create_engine(DB_URL)

def ingest(engine):
    """
    Load both Excel files into their raw staging tables.
    """

    # Read transaction data from Excel
    df_tx = pd.read_excel('transactions_2_11211.xlsx')

    # Normalize selected text fields to uppercase
    tx_upper_cols = [
        'address_line_1', 'address_line_2', 'city', 'state'
    ]
    for col in tx_upper_cols:
        if col in df_tx.columns:
            df_tx[col] = df_tx[col].fillna('').astype(str).str.strip().str.upper()

    # Insert transaction data into raw_transactions table
    df_tx.to_sql('raw_transactions', engine, if_exists='replace', index=False)

    # Read canonical address data from Excel
    df_addr = pd.read_excel('11211 Addresses.xlsx')

    # Normalize selected address fields to uppercase
    addr_upper_cols = [
        'street', 'strtype', 'apttype', 'aptnbr',
        'predir', 'postdir', 'city', 'state'
    ]
    for col in addr_upper_cols:
        if col in df_addr.columns:
            df_addr[col] = df_addr[col].fillna('').astype(str).str.strip().str.upper()

    # Insert address data into raw_addresses table
    df_addr.to_sql('raw_addresses', engine, if_exists='replace', index=False)
