import pandas as pd
from datetime import datetime
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables from config directory
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', '.env')
load_dotenv(env_path)

def get_db_engine():
    """Create database engine from environment variables"""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    return create_engine(db_url)

def load_invoices_to_staging(file_path: str):
    """Load invoices data from CSV to staging table"""
    # Read CSV file with appropriate encoding for UK pound symbol
    df = pd.read_csv(file_path, encoding='latin1')
    
    # Data transformations
    df['invoice_date'] = pd.to_datetime(df['InvoiceDate'])
    df['created_at'] = datetime.now()
    df['batch_id'] = str(uuid.uuid4())
    
    # Rename columns to match staging table
    df = df.rename(columns={
        'Invoice': 'invoice_number',
        'StockCode': 'stock_code',
        'Description': 'description',
        'Quantity': 'quantity',
        'Price': 'unit_price',
        'Customer ID': 'customer_id',
        'Country': 'country'
    })
    
    # Select and reorder columns to match staging table
    columns = [
        'invoice_number', 'stock_code', 'description', 'quantity',
        'invoice_date', 'unit_price', 'customer_id', 'country',
        'created_at', 'batch_id'
    ]
    df = df[columns]
    
    # Load to database
    engine = get_db_engine()
    df.to_sql('stg_invoices', engine, schema='staging', if_exists='append', index=False)
    
    return len(df)

if __name__ == '__main__':
    data_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                            'data', 'Invoices_Year_2009-2010.csv')
    rows_loaded = load_invoices_to_staging(data_file)
    print(f"Loaded {rows_loaded} rows to staging table")
