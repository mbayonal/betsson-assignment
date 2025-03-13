from datetime import datetime
from sqlalchemy.orm import Session
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from core.dimensions.dim_date import DimDate
from utils.db_connection import create_db_engine
from core.staging.stage_sales import StageSales

class DateTransformer:
    """Transform InvoiceDate from staging into date dimension."""
    
    def __init__(self):
        self.engine = create_db_engine()
        self._day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        self._month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                           'July', 'August', 'September', 'October', 'November', 'December']
    
    def transform_dates(self) -> int:
        """
        Transform unique dates from staging into date dimension.
        Returns:
            Number of dates processed
        """
        with Session(self.engine) as session:
            # Get unique dates from staging that haven't been processed
            unique_dates = session.query(StageSales.invoice_date)\
                .filter(StageSales.is_processed == False)\
                .distinct()\
                .all()
            
            dates_processed = 0
            for (invoice_date,) in unique_dates:
                try:
                    # Parse MM/DD/YYYY HH:MM format
                    date_obj = datetime.strptime(invoice_date, '%m/%d/%Y %H:%M')
                    
                    # Create date key in YYYYMMDD format
                    date_key = int(date_obj.strftime('%Y%m%d'))
                    
                    # Skip if date already exists
                    if session.query(DimDate).filter_by(date_key=date_key).first():
                        continue
                    
                    # Create new date dimension record
                    dim_date = DimDate(
                        date_key=date_key,
                        full_date=date_obj.date(),
                        year=date_obj.year,
                        month=date_obj.month,
                        day=date_obj.day,
                        hour=date_obj.hour,
                        minute=date_obj.minute,
                        day_of_week=date_obj.isoweekday(),
                        day_name=self._day_names[date_obj.weekday()],
                        month_name=self._month_names[date_obj.month - 1],
                        quarter=((date_obj.month - 1) // 3) + 1,
                        is_weekend=date_obj.isoweekday() >= 6,
                        is_business_day=date_obj.isoweekday() <= 5,
                        fiscal_year=date_obj.year + (1 if date_obj.month >= 10 else 0),
                        fiscal_quarter=((date_obj.month - 1) // 3) + 1,
                        valid_from=datetime.now(),
                        is_current=True
                    )
                    
                    session.add(dim_date)
                    dates_processed += 1
                    
                    # Commit every 100 dates
                    if dates_processed % 100 == 0:
                        session.commit()
                
                except ValueError as e:
                    print(f"Error processing date {invoice_date}: {str(e)}")
                    continue
            
            session.commit()
            return dates_processed
