from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, DateTime, func

# Create base with shared metadata
Base = declarative_base()

class FactBase(Base):
    """Base class for all fact tables."""
    __abstract__ = True
    
    # Common columns for all fact tables
    fact_key = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(), onupdate=func.now())
