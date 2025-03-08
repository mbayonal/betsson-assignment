from sqlalchemy import Column, Integer, String, Numeric, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StgInvoices(Base):
    __tablename__ = 'stg_invoices'
    
    # Using composite primary key since staging tables often mirror source data
    id = Column(Integer, primary_key=True, autoincrement=True)
    invoice_number = Column(String(50), nullable=False)
    stock_code = Column(String(50), nullable=False)
    description = Column(String(255), nullable=True)
    quantity = Column(Integer, nullable=False)
    invoice_date = Column(DateTime, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    customer_id = Column(Integer, nullable=False)
    country = Column(String(100), nullable=False)
    
    # Audit columns
    created_at = Column(DateTime, nullable=False)
    batch_id = Column(String(50), nullable=False)
