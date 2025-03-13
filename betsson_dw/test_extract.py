import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from core.staging.stage_sales import StageSales
from datetime import datetime

# Configuration
file_path = 'data/Invoices_Year_2009-2010.csv'
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://betsson_user:betsson_password@localhost/betsson_dw')

# Verify file
print(f"File exists: {os.path.exists(file_path)}")
print(f"File size: {os.path.getsize(file_path)} bytes")

# Read CSV
df = pd.read_csv(file_path, encoding='ISO-8859-1', header=0)
print(f"Read {len(df)} rows from CSV")
print(f"CSV columns: {df.columns.tolist()}")
print("First row:", df.iloc[0].to_dict())

# Map columns
column_mapping = {
    'Invoice': 'invoice',
    'StockCode': 'stock_code',
    'Description': 'description',
    'Quantity': 'quantity',
    'InvoiceDate': 'invoice_date',
    'Price': 'price',
    'Customer ID': 'customer_id',
    'Country': 'country'
}
df = df.rename(columns=column_mapping)
print(f"After renaming: {df.columns.tolist()}")

# Clean data
required_columns = ['invoice', 'stock_code', 'quantity', 'invoice_date']
print(f"Rows before cleaning: {len(df)}")
df = df.dropna(subset=required_columns)
print(f"Rows after cleaning: {len(df)}")

# Insert test record manually
engine = create_engine(DATABASE_URL)
with Session(engine) as session:
    # Create a single test record
    test_record = StageSales(
        invoice="TEST-MANUAL",
        stock_code="TEST123",
        description="Test Item",
        quantity=1,
        invoice_date="01/01/2020 12:00",
        price=9.99,
        customer_id="CUST-001",
        country="Testland",
        file_name=file_path,
        load_date=datetime.now(),
        is_processed=False
    )
    session.add(test_record)
    session.commit()
    print("Manual test record inserted")
    
    # Try inserting first CSV record
    if len(df) > 0:
        row = df.iloc[0]
        try:
            csv_record = StageSales(
                invoice=str(row['invoice']).strip(),
                stock_code=str(row['stock_code']).strip(),
                description=str(row['description']).strip() if pd.notna(row['description']) else None,
                quantity=int(row['quantity']),
                invoice_date=str(row['invoice_date']).strip(),
                price=float(row['price']) if pd.notna(row['price']) else None,
                customer_id=str(row['customer_id']) if pd.notna(row['customer_id']) else None,
                country=str(row['country']).strip() if pd.notna(row['country']) else None,
                file_name=file_path,
                load_date=datetime.now(),
                is_processed=False
            )
            session.add(csv_record)
            session.commit()
            print("First CSV record inserted successfully")
        except Exception as e:
            print(f"Error inserting CSV record: {str(e)}")
            session.rollback()
