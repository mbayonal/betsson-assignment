from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Boolean, func

# Create base with shared metadata
Base = declarative_base()

class DimensionBase(Base):
    """Base class for all dimension tables."""
    __abstract__ = True
    
    # SCD Type 2 tracking columns
    valid_from = Column(DateTime, nullable=False, default=func.now())
    valid_to = Column(DateTime, nullable=True)
    is_current = Column(Boolean, nullable=False, default=True)
