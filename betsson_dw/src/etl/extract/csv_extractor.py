import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import pandas as pd
from datetime import datetime
from sqlalchemy.orm import Session
from core.staging.stage_sales import StageSales
from utils.db_connection import create_db_engine, create_engine, get_connection_string
from sqlalchemy import event

class CsvExtractor:
    """Extract data from Invoices_Year_2009-2010.csv to staging."""
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.engine = create_db_engine()
    
    def extract(self):
        """Extract data from CSV file to staging."""
        try:
            print(f"CSV file path: {os.path.abspath(self.file_path)}")
            print(f"File exists: {os.path.exists(self.file_path)}")
            print(f"File size: {os.path.getsize(self.file_path)} bytes")
            
            # Read CSV with proper encoding for special characters
            df = pd.read_csv(
                self.file_path,
                encoding='ISO-8859-1',
                header=0
            )
            df = df.where(pd.notnull(df), None)
            print(f'Loaded {len(df)} records from CSV')
            print('Sample data:', df.iloc[0].to_dict())
            
            # Map columns properly
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
            print(f"Columns after mapping: {df.columns.tolist()}")
            
            # Validate required columns
            required_columns = ['invoice', 'stock_code', 'quantity', 'invoice_date']
            missing_cols = [col for col in required_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f'Missing required columns: {missing_cols}')
            
            # Check for nulls in required columns
            print(f"Rows before cleaning: {len(df)}")
            null_counts = df[required_columns].isnull().sum()
            print(f"Null counts in required columns: {null_counts.to_dict()}")
            
            # Drop rows with nulls in required columns
            df = df.dropna(subset=required_columns)
            print(f"Rows after cleaning: {len(df)}")
            
            print('DataFrame Info:')
            print(df.info())
            print('\nData Description:')
            print(df[['invoice', 'stock_code', 'quantity', 'price']].describe())
            
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
            df['price'] = pd.to_numeric(df['price'], errors='coerce').astype(float)
            print('Null values after conversion:', df.isnull().sum())
            
            # Create database session with explicit transaction control
            connection_string = get_connection_string()
            engine = create_engine(connection_string, isolation_level="READ COMMITTED")
            with Session(engine) as session:
                try:
                    # Process in smaller batches for better error handling
                    batch_size = 100  # Reduced batch size
                    records_processed = 0
                    
                    # Create a direct connection for raw SQL if needed
                    connection = engine.connect()
                    
                    # Process each row individually for better error control
                    total_rows = len(df)
                    print(f"Starting to process {total_rows} rows...")
                    
                    for index, row in df.iterrows():
                        try:
                            # Create record with proper type handling
                            stage_record = {
                                'invoice': str(row['invoice']).strip(),
                                'stock_code': str(row['stock_code']).strip(),
                                'description': str(row['description']).strip() if pd.notna(row['description']) else None,
                                'quantity': int(row['quantity']),
                                'invoice_date': str(row['invoice_date']).strip(),
                                'price': float(row['price']) if pd.notna(row['price']) else None,
                                'customer_id': str(row['customer_id']) if pd.notna(row['customer_id']) else None,
                                'country': str(row['country']).strip() if pd.notna(row['country']) else None,
                                'file_name': self.file_path,
                                'load_date': datetime.now(),
                                'is_processed': False  # Will be properly cast by SQLAlchemy
                            }
                            
                            # Insert using SQLAlchemy Core for better type control
                            from sqlalchemy import Table, MetaData, Column, Integer, String, Boolean, Float, DateTime
                            from sqlalchemy.sql import insert
                            
                            metadata = MetaData(schema='staging')
                            stage_sales_table = Table(
                                'stage_sales', 
                                metadata,
                                Column('invoice', String),
                                Column('stock_code', String),
                                Column('description', String),
                                Column('quantity', Integer),
                                Column('invoice_date', String),
                                Column('price', Float),
                                Column('customer_id', String),
                                Column('country', String),
                                Column('file_name', String),
                                Column('load_date', DateTime),
                                Column('is_processed', Boolean)
                            )
                            
                            # Execute insert
                            ins = insert(stage_sales_table).values(**stage_record)
                            connection.execute(ins)
                            
                            records_processed += 1
                            
                            # Commit every batch_size records
                            if records_processed % batch_size == 0:
                                connection.commit()
                                print(f"Processed {records_processed}/{total_rows} records ({(records_processed/total_rows)*100:.1f}%)")
                                
                        except Exception as e:
                            print(f"Error processing row {index} (record {records_processed}): {str(e)}")
                            print(f"Problematic data: {row.to_dict()}")
                            # Continue processing other rows
                            continue
                    
                    # Commit any remaining records
                    connection.commit()
                    print(f"Completed processing {records_processed}/{total_rows} records")
                    
                    # Verify data was inserted
                    from sqlalchemy import text
                    count_query = text("SELECT COUNT(*) FROM staging.stage_sales")
                    count_result = connection.execute(count_query).scalar()
                    print(f"Total records in staging table: {count_result}")
                    
                    # Close connection
                    connection.close()
                    
                    return records_processed
                except Exception as e:
                    print(f"Database error: {str(e)}")
                    session.rollback()
                    raise
        except Exception as e:
            print(f"Extraction error: {str(e)}")
            raise
        finally:
            if 'engine' in locals():
                engine.dispose()
                print('Connection pool closed:', engine.pool.status())
