from sqlalchemy import Column, String, DateTime, Boolean
from datetime import datetime
from .dim_base import DimensionBase

class DimCustomer(DimensionBase):
    """Customer dimension from Customer ID and Country fields"""
    __tablename__ = 'dim_customer'
    __table_args__ = {'schema': 'dimensions'}
    
    # Surrogate key
    customer_key = Column(String(50), primary_key=True)
    
    # Business key from source
    customer_id = Column(String(50), nullable=True)  # Nullable as per source data
    
    # Attributes from source
    country = Column(String(100))
    
    # SCD Type 2 tracking
    valid_from = Column(DateTime, nullable=False, default=datetime.now)
    valid_to = Column(DateTime)
    is_current = Column(Boolean, nullable=False, default=True)
