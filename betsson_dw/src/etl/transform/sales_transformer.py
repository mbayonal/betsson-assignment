from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from sqlalchemy.orm import Session
from sqlalchemy import select
from core.facts.fact_sales import FactSales
from core.dimensions.dim_date import DimDate
from core.dimensions.dim_product import DimProduct
from core.dimensions.dim_customer import DimCustomer
from core.staging.stage_sales import StageSales
from utils.db_connection import create_db_engine
import uuid
from sqlalchemy import text

class SalesTransformer:
    """Transform sales data from staging into fact table."""
    
    def __init__(self):
        self.engine = create_db_engine()
    
    def transform_sales(self, batch_size: int = 1000) -> int:
        """
        Transform sales data from staging into fact table.
        Args:
            batch_size: Number of records to process in each batch
        Returns:
            Number of sales processed
        """
        sales_processed = 0
        
        # Ensure we have an ANONYMOUS customer record
        with Session(self.engine) as session:
            # Check if ANONYMOUS customer exists
            anon_customer = session.query(DimCustomer).filter_by(customer_key='ANONYMOUS').first()
            if not anon_customer:
                # Create ANONYMOUS customer
                anon_customer = DimCustomer(
                    customer_key='ANONYMOUS',
                    customer_id=None,
                    country='Unknown',
                    valid_from=datetime.now(),
                    is_current=True
                )
                session.add(anon_customer)
                session.commit()
                print("Created ANONYMOUS customer record")
        
        # Get unprocessed sales from staging
        with Session(self.engine) as session:
            unprocessed_sales = session.query(StageSales)\
                .filter_by(is_processed=False)\
                .all()
            
            print(f"Found {len(unprocessed_sales)} unprocessed sales records")
        
        # Process each sale in its own transaction
        for sale in unprocessed_sales:
            try:
                with Session(self.engine) as session:
                    # Get date key
                    date_obj = datetime.strptime(sale.invoice_date, '%m/%d/%Y %H:%M')
                    date_key = int(date_obj.strftime('%Y%m%d'))
                    
                    # Verify date exists
                    date_exists = session.query(DimDate).filter_by(date_key=date_key).first()
                    if not date_exists:
                        print(f"Warning: Date key {date_key} not found for invoice_date {sale.invoice_date}")
                        # Skip this record - we should run the date transformer first
                        continue
                    
                    # Get product key (current version)
                    product = session.query(DimProduct)\
                        .filter_by(stock_code=sale.stock_code, is_current=True)\
                        .first()
                    if not product:
                        print(f"Warning: Product not found for stock_code {sale.stock_code}")
                        continue
                    
                    # Get customer key (current version or anonymous)
                    customer_key = 'ANONYMOUS'
                    if sale.customer_id:
                        customer = session.query(DimCustomer)\
                            .filter_by(customer_id=sale.customer_id, is_current=True)\
                            .first()
                        if customer:
                            customer_key = customer.customer_key
                    
                    # Verify customer key exists
                    customer_exists = session.query(DimCustomer).filter_by(customer_key=customer_key).first()
                    if not customer_exists:
                        print(f"Warning: Customer key {customer_key} not found")
                        customer_key = 'ANONYMOUS'
                    
                    # Calculate values
                    unit_price = sale.price if sale.price is not None else 0.0
                    total_amount = sale.quantity * unit_price
                    
                    # Use direct SQL to insert fact record
                    stmt = text("""
                    INSERT INTO facts.fact_sales 
                    (invoice_number, date_key, product_key, customer_key, quantity, unit_price, total_amount, created_at)
                    VALUES (:invoice, :date_key, :product_key, :customer_key, :quantity, :unit_price, :total_amount, :created_at)
                    """)
                    
                    session.execute(stmt, {
                        'invoice': sale.invoice,
                        'date_key': date_key,
                        'product_key': product.product_key,
                        'customer_key': customer_key,
                        'quantity': sale.quantity,
                        'unit_price': unit_price,
                        'total_amount': total_amount,
                        'created_at': datetime.now()
                    })
                    
                    # Mark staging record as processed
                    staging_record = session.query(StageSales).filter_by(stage_key=sale.stage_key).first()
                    if staging_record:
                        staging_record.is_processed = True
                    
                    # Commit this transaction
                    session.commit()
                    sales_processed += 1
                    
                    # Print progress
                    if sales_processed % batch_size == 0:
                        print(f"Processed {sales_processed} sales records")
                
            except Exception as e:
                print(f"Error processing sale {sale.invoice}: {str(e)}")
                continue
        
        print(f"Sales transformation complete: {sales_processed} sales processed")
        return sales_processed
