from sqlalchemy import Column, Integer, String, DateTime, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StageSales(Base):
    """Staging table for raw sales data from Invoices_Year_2009-2010.csv"""
    __tablename__ = 'stage_sales'
    __table_args__ = {'schema': 'staging'}
    
    # Surrogate key for staging
    stage_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Direct mapping of source fields
    invoice = Column(String(50), nullable=False)
    stock_code = Column(String(50), nullable=False)
    description = Column(String(500))
    quantity = Column(Integer, nullable=False)
    invoice_date = Column(String(50), nullable=False)  # Raw date string for validation
    price = Column(Numeric(10, 2))
    customer_id = Column(String(50))
    country = Column(String(100))
    
    # ETL tracking columns
    file_name = Column(String(200), nullable=False)
    load_date = Column(DateTime, nullable=False)
    is_processed = Column(Integer, default=0)
