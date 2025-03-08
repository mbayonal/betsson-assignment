from datetime import datetime
from sqlalchemy import select, update
from ..utils.db_connection import get_db_session
from ..staging.stage_customer import StageCustomer
from ..dimensions.dim_customer import DimCustomer

class CustomerETL:
    def __init__(self):
        self.session = get_db_session()

    def process_staged_customers(self):
        """Process staged customer records into the customer dimension."""
        # Get unprocessed staged records
        staged_records = self.session.query(StageCustomer).filter_by(processed=0).all()
        
        for staged_record in staged_records:
            # Check if customer already exists
            existing_customer = self.session.query(DimCustomer)\
                .filter_by(customer_id=staged_record.source_customer_id, is_current=1)\
                .first()
            
            if existing_customer:
                # Implement SCD Type 2 logic
                if self._has_changes(existing_customer, staged_record):
                    # Expire current record
                    existing_customer.is_current = 0
                    existing_customer.effective_end_date = datetime.now()
                    
                    # Create new record
                    self._create_new_customer_record(staged_record)
            else:
                # Create new customer record
                self._create_new_customer_record(staged_record)
            
            # Mark staged record as processed
            staged_record.processed = 1
        
        self.session.commit()

    def _has_changes(self, existing, staged):
        """Compare relevant fields to detect changes."""
        return any([
            existing.first_name != staged.first_name,
            existing.last_name != staged.last_name,
            existing.email != staged.email,
            existing.date_of_birth != staged.date_of_birth,
            existing.country != staged.country,
            existing.city != staged.city
        ])

    def _create_new_customer_record(self, staged):
        """Create a new customer dimension record."""
        new_customer = DimCustomer(
            customer_id=staged.source_customer_id,
            first_name=staged.first_name,
            last_name=staged.last_name,
            email=staged.email,
            date_of_birth=staged.date_of_birth,
            country=staged.country,
            city=staged.city,
            effective_start_date=datetime.now(),
            is_current=1,
            source_system='STAGE'
        )
        self.session.add(new_customer)
