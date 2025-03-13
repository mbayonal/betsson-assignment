from sqlalchemy import Column, Integer, String, Numeric, ForeignKey, DateTime
from datetime import datetime
from .fact_base import FactBase

class FactSales(FactBase):
    """Sales fact table from Invoice, StockCode, Quantity, Price fields"""
    __tablename__ = 'fact_sales'
    __table_args__ = {'schema': 'facts'}
    
    # Surrogate key
    fact_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Business key from source
    invoice_number = Column(String(50), nullable=False)
    
    # Foreign keys to dimensions
    date_key = Column(Integer, ForeignKey('dimensions.dim_date.date_key'), nullable=False)
    product_key = Column(String(50), ForeignKey('dimensions.dim_product.product_key'), nullable=False)
    customer_key = Column(String(50), ForeignKey('dimensions.dim_customer.customer_key'), nullable=True)  # Nullable as per source
    
    # Measures from source
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    total_amount = Column(Numeric(12, 2), nullable=False)  # Calculated as quantity * unit_price
    
    # Audit columns
    created_at = Column(DateTime, nullable=False, default=datetime.now)
