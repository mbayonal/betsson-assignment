from sqlalchemy import Column, Integer, String, Date, DateTime, Boolean
from src.dimensions.dim_base import DimensionBase

class DimCustomer(DimensionBase):
    """Customer dimension table following Kimball architecture."""
    __tablename__ = 'dim_customer'
    __table_args__ = {'schema': 'dimensions'}
    
    # Primary key (surrogate key)
    customer_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Business key and attributes
    customer_id = Column(String(50), nullable=False)  # Natural/business key
    country = Column(String(50), nullable=False)  # Required attribute
    
    # SCD Type 2 tracking columns
    valid_from = Column(DateTime, nullable=False)
    valid_to = Column(DateTime)
    is_current = Column(Boolean, nullable=False, default=True)
