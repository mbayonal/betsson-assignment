from sqlalchemy import Column, Integer, String, Date, Boolean
from .dim_base import DimensionBase

class DimDate(DimensionBase):
    """Date dimension derived from InvoiceDate field (MM/DD/YYYY HH:MM format)"""
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'dimensions'}
    
    # Primary key (YYYYMMDD format)
    date_key = Column(Integer, primary_key=True)
    
    # Date components from InvoiceDate
    full_date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    day = Column(Integer, nullable=False)
    hour = Column(Integer, nullable=False)
    minute = Column(Integer, nullable=False)
    
    # Time attributes
    day_of_week = Column(Integer, nullable=False)  # 1=Monday, 7=Sunday
    day_name = Column(String(10), nullable=False)  # Monday, Tuesday, etc.
    month_name = Column(String(10), nullable=False)  # January, February, etc.
    quarter = Column(Integer, nullable=False)  # 1-4
    
    # Business calendar
    is_weekend = Column(Boolean, nullable=False)
    is_business_day = Column(Boolean, nullable=False)
    fiscal_year = Column(Integer, nullable=False)  # Year + 1 if month >= 10
    fiscal_quarter = Column(Integer, nullable=False)  # 1-4
