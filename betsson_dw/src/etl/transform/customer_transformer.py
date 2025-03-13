from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from core.dimensions.dim_customer import DimCustomer
from utils.db_connection import create_db_engine
from core.staging.stage_sales import StageSales
from sqlalchemy.orm import Session
from sqlalchemy import select

class CustomerTransformer:
    """Transform customer data from staging into customer dimension."""
    
    def __init__(self):
        self.engine = create_db_engine()
    
    def transform_customers(self) -> int:
        """
        Transform unique customers from staging into customer dimension.
        Returns:
            Number of customers processed
        """
        with Session(self.engine) as session:
            # Get unique customers from staging that haven't been processed
            unique_customers = session.query(
                StageSales.customer_id,
                StageSales.country
            ).filter(
                StageSales.is_processed == False
            ).distinct().all()
            
            customers_processed = 0
            for customer_id, country in unique_customers:
                try:
                    # Handle null customer_id (anonymous customers)
                    if not customer_id:
                        continue  # Skip anonymous customers
                    
                    # Check for existing customer
                    existing_customer = session.query(DimCustomer)\
                        .filter_by(customer_id=customer_id, is_current=True)\
                        .first()
                    
                    # Handle SCD Type 2 changes
                    if existing_customer:
                        # Check if customer details have changed
                        if existing_customer.country != country:
                            # Expire current record
                            existing_customer.is_current = False
                            existing_customer.valid_to = datetime.now()
                            
                            # Create new record
                            new_customer = DimCustomer(
                                customer_key=f"{customer_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                                customer_id=customer_id,
                                country=country,
                                valid_from=datetime.now(),
                                is_current=True
                            )
                            session.add(new_customer)
                            customers_processed += 1
                    else:
                        # Create new customer record
                        new_customer = DimCustomer(
                            customer_key=customer_id,  # Use customer_id as initial key
                            customer_id=customer_id,
                            country=country,
                            valid_from=datetime.now(),
                            is_current=True
                        )
                        session.add(new_customer)
                        customers_processed += 1
                    
                    # Commit every 100 customers
                    if customers_processed % 100 == 0:
                        session.commit()
                
                except Exception as e:
                    print(f"Error processing customer {customer_id}: {str(e)}")
                    continue
            
            # Create a single record for anonymous customers if it doesn't exist
            anon_customer = session.query(DimCustomer)\
                .filter_by(customer_id=None, is_current=True)\
                .first()
            
            if not anon_customer:
                anon_customer = DimCustomer(
                    customer_key='ANONYMOUS',
                    customer_id=None,
                    country='Unknown',
                    valid_from=datetime.now(),
                    is_current=True
                )
                session.add(anon_customer)
                customers_processed += 1
            
            session.commit()
            return customers_processed
