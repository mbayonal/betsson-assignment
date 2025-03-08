from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, func
from src.utils.metadata import metadata

# Create base with shared metadata
Base = declarative_base(metadata=metadata)

class FactBase(Base):
    """Base class for all fact tables."""
    __abstract__ = True
    
    # Audit columns
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())
