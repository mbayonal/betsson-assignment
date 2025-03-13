from datetime import datetime
import sys
import os
import uuid
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from sqlalchemy.orm import Session
from sqlalchemy import select
from core.dimensions.dim_product import DimProduct
from core.staging.stage_sales import StageSales
from utils.db_connection import create_db_engine

class ProductTransformer:
    """Transform product data from staging into product dimension."""
    
    def __init__(self):
        self.engine = create_db_engine()
    
    def transform_products(self) -> int:
        """
        Transform unique products from staging into product dimension.
        Returns:
            Number of products processed
        """
        with Session(self.engine) as session:
            # Get unique products from staging that haven't been processed
            unique_products = session.query(
                StageSales.stock_code,
                StageSales.description,
                StageSales.price
            ).filter(
                StageSales.is_processed == False
            ).distinct().all()
            
            products_processed = 0
            for stock_code, description, price in unique_products:
                try:
                    # Handle NULL price
                    unit_price = price if price is not None else 0.0
                    
                    # Check for existing product
                    existing_product = session.query(DimProduct)\
                        .filter_by(stock_code=stock_code, is_current=True)\
                        .first()
                    
                    # Handle SCD Type 2 changes
                    if existing_product:
                        # Check if product details have changed
                        if (existing_product.description != description or
                            existing_product.unit_price != unit_price):
                            # Expire current record
                            existing_product.is_current = False
                            existing_product.valid_to = datetime.now()
                            
                            # Create new record with unique key
                            new_product = DimProduct(
                                product_key=f"{stock_code}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}",
                                stock_code=stock_code,
                                description=description,
                                unit_price=unit_price,
                                valid_from=datetime.now(),
                                is_current=True
                            )
                            session.add(new_product)
                            products_processed += 1
                    else:
                        # Create new product record
                        new_product = DimProduct(
                            product_key=f"{stock_code}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{str(uuid.uuid4())[:8]}",  
                            stock_code=stock_code,
                            description=description,
                            unit_price=unit_price,
                            valid_from=datetime.now(),
                            is_current=True
                        )
                        session.add(new_product)
                        products_processed += 1
                    
                    # Commit every 100 products
                    if products_processed % 100 == 0:
                        session.commit()
                
                except Exception as e:
                    print(f"Error processing product {stock_code}: {str(e)}")
                    continue
            
            session.commit()
            return products_processed
