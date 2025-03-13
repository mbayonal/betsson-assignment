from sqlalchemy import Column, String, Numeric, DateTime, Boolean
from datetime import datetime
from .dim_base import DimensionBase

class DimProduct(DimensionBase):
    """Product dimension from StockCode, Description, Price fields"""
    __tablename__ = 'dim_product'
    __table_args__ = {'schema': 'dimensions'}
    
    # Surrogate key
    product_key = Column(String(50), primary_key=True)
    
    # Business key from source
    stock_code = Column(String(50), nullable=False, unique=True)
    
    # Descriptive fields from source
    description = Column(String(500))
    unit_price = Column(Numeric(10, 2))
    
    # SCD Type 2 tracking
    valid_from = Column(DateTime, nullable=False, default=datetime.now)
    valid_to = Column(DateTime)
    is_current = Column(Boolean, nullable=False, default=True)
