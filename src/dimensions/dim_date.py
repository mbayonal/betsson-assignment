from sqlalchemy import Column, Integer, String, Date
from .dim_base import DimensionBase

class DimDate(DimensionBase):
    __tablename__ = 'dim_date'
    __table_args__ = {'schema': 'dimensions'}
    
    date_key = Column(Integer, primary_key=True)  # YYYYMMDD format
    date = Column(Date, nullable=False, unique=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    month_name = Column(String(9), nullable=False)  # September is longest
    quarter = Column(Integer, nullable=False)
    year_quarter = Column(String(7), nullable=False)  # YYYY-QN format
    day_of_week = Column(Integer, nullable=False)
    day_name = Column(String(9), nullable=False)  # Wednesday is longest
    is_weekend = Column(Integer, nullable=False)  # 0 or 1
