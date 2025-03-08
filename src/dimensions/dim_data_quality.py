from sqlalchemy import Column, Integer, String, DateTime
from src.dimensions.dim_base import DimensionBase

class DimDataQuality(DimensionBase):
    """Data quality dimension table following Kimball architecture."""
    __tablename__ = 'dim_data_quality'
    __table_args__ = {'schema': 'dimensions'}
    
    # Primary key
    data_quality_key = Column(Integer, primary_key=True, autoincrement=True)
    
    # Business keys and attributes
    issue_type = Column(String(50), nullable=False)  # e.g., 'NEGATIVE_QUANTITY', 'ZERO_PRICE'
    severity = Column(String(20), nullable=False)  # e.g., 'HIGH', 'MEDIUM', 'LOW'
    action_taken = Column(String(50), nullable=False)  # e.g., 'FILTERED', 'ADJUSTED', 'FLAGGED'
    description = Column(String(255), nullable=False)
