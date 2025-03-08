from sqlalchemy import Column, Integer, String, Date, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class StageCustomer(Base):
    __tablename__ = 'stage_customer'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_customer_id = Column(String(50), nullable=False)
    first_name = Column(String(100))
    last_name = Column(String(100))
    email = Column(String(255))
    date_of_birth = Column(Date)
    country = Column(String(100))
    city = Column(String(100))
    loaded_at = Column(DateTime, default=func.now())
    processed = Column(Integer, default=0)  # Flag to track ETL processing status
