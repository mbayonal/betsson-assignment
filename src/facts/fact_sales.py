from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from src.facts.fact_base import FactBase

class FactSales(FactBase):
    """Fact sales table following Kimball architecture."""
    __tablename__ = 'fact_sales'
    __table_args__ = {'schema': 'facts'}
    
    # Primary key
    sales_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Business keys
    invoice_number = Column(String(50), nullable=False)
    stock_code = Column(String(50), nullable=False)
    description = Column(String(255), nullable=True)
    batch_id = Column(String(50), nullable=False)
    
    # Foreign keys
    customer_key = Column(Integer, ForeignKey('dimensions.dim_customer.customer_key'), nullable=False)
    date_key = Column(Integer, ForeignKey('dimensions.dim_date.date_key'), nullable=False)
    data_quality_key = Column(Integer, ForeignKey('dimensions.dim_data_quality.data_quality_key'), nullable=False)
    
    # Measures
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    total_amount = Column(Float, nullable=False)
    
    # Additional timestamps
    invoice_date = Column(DateTime, nullable=False)
