import os
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def populate_date_dimension(engine, start_date, end_date):
    """Populate the date dimension with dates between start_date and end_date."""
    print("Populating date dimension...")
    
    # Generate dates
    dates = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Create DataFrame with date attributes
    df = pd.DataFrame({
        'date_key': dates.strftime('%Y%m%d').astype(int),
        'date': dates.date,
        'year': dates.year,
        'month': dates.month,
        'month_name': dates.strftime('%B'),
        'quarter': dates.quarter,
        'year_quarter': dates.strftime('%Y') + '-Q' + (((dates.month-1) // 3) + 1).astype(str),
        'day_of_week': dates.dayofweek,
        'day_name': dates.strftime('%A'),
        'is_weekend': (dates.dayofweek >= 5).astype(int),
        # Add SCD Type 2 tracking columns
        'valid_from': datetime.now(),
        'valid_to': None,
        'is_current': True,
        # Add audit columns
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    })
    
    # Load to database
    df.to_sql('dim_date', engine, schema='dimensions', if_exists='append', index=False)
    return len(df)

def populate_customer_dimension(engine):
    """Populate the customer dimension from staging data."""
    print("Populating customer dimension...")
    
    # Create DataFrame for unknown customer
    unknown_customer = pd.DataFrame([{
        'customer_id': 'UNKNOWN',
        'country': 'UNKNOWN',
        'valid_from': datetime.now(),
        'valid_to': None,
        'is_current': True,
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    }])
    
    # Load unknown customer first
    unknown_customer.to_sql('dim_customer', engine, schema='dimensions', if_exists='append', index=False)
    print("Added unknown customer record")
    
    # Get unique customers from staging
    query = """
    SELECT DISTINCT
        customer_id::text as customer_id,
        country
    FROM staging.stg_invoices
    WHERE customer_id IS NOT NULL 
      AND country IS NOT NULL
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Add SCD Type 2 tracking and audit columns
    now = datetime.now()
    df['valid_from'] = now
    df['valid_to'] = None
    df['is_current'] = True
    df['created_at'] = now
    df['updated_at'] = now
    
    # Load to database
    df.to_sql('dim_customer', engine, schema='dimensions', if_exists='append', index=False)
    return len(df)

def populate_data_quality_dimension(engine):
    """Populate the data quality dimension with issue types."""
    print("Populating data quality dimension...")
    
    # Define data quality rules
    quality_rules = [
        {
            'issue_type': 'VALID_RECORD',
            'severity': 'NONE',
            'action_taken': 'NONE',
            'description': 'Record passed all data quality checks'
        },
        {
            'issue_type': 'NEGATIVE_QUANTITY',
            'severity': 'HIGH',
            'action_taken': 'ADJUSTED_TO_ZERO',
            'description': 'Negative quantity was found and adjusted to zero'
        },
        {
            'issue_type': 'ZERO_PRICE',
            'severity': 'MEDIUM',
            'action_taken': 'FLAGGED',
            'description': 'Zero or negative unit price was found'
        },
        {
            'issue_type': 'NEGATIVE_AMOUNT',
            'severity': 'HIGH',
            'action_taken': 'RECALCULATED',
            'description': 'Negative total amount was recalculated from quantity and unit price'
        }
    ]
    
    # Create DataFrame
    df = pd.DataFrame(quality_rules)
    
    # Add SCD Type 2 tracking and audit columns
    now = datetime.now()
    df['valid_from'] = now
    df['valid_to'] = None
    df['is_current'] = True
    df['created_at'] = now
    df['updated_at'] = now
    
    # Load to database
    df.to_sql('dim_data_quality', engine, schema='dimensions', if_exists='append', index=False)
    return len(df)

def populate_fact_sales(engine):
    """Populate the fact sales table from staging data."""
    print("Populating fact sales...")
    
    # Get data quality keys
    dq_query = """
    SELECT 
        data_quality_key,
        issue_type
    FROM dimensions.dim_data_quality
    WHERE is_current = true
    """
    
    with engine.connect() as conn:
        dq_df = pd.read_sql(dq_query, conn)
    
    dq_keys = dict(zip(dq_df['issue_type'], dq_df['data_quality_key']))
    
    # Join with dimension tables to get surrogate keys
    query = """
    WITH sales_data AS (
        SELECT 
            s.invoice_number,
            s.stock_code,
            s.description,
            COALESCE(c.customer_key, 
                     (SELECT customer_key 
                      FROM dimensions.dim_customer 
                      WHERE customer_id = 'UNKNOWN' 
                        AND is_current = true)
            ) as customer_key,
            TO_NUMBER(TO_CHAR(s.invoice_date, 'YYYYMMDD'), '99999999') as date_key,
            s.quantity,
            s.unit_price,
            s.quantity * s.unit_price as total_amount,
            s.invoice_date,
            COALESCE(s.batch_id, 'UNKNOWN') as batch_id,
            CASE 
                WHEN s.quantity < 0 THEN 'NEGATIVE_QUANTITY'
                WHEN s.unit_price <= 0 THEN 'ZERO_PRICE'
                WHEN s.quantity * s.unit_price < 0 THEN 'NEGATIVE_AMOUNT'
                ELSE 'VALID_RECORD'
            END as issue_type
        FROM staging.stg_invoices s
        LEFT JOIN dimensions.dim_customer c 
            ON s.customer_id::text = c.customer_id
            AND c.is_current = true
    )
    SELECT
        invoice_number,
        stock_code,
        description,
        customer_key,
        date_key,
        CASE WHEN quantity < 0 THEN 0 ELSE quantity END as quantity,
        GREATEST(unit_price, 0) as unit_price,
        CASE 
            WHEN quantity < 0 OR unit_price <= 0 THEN 0
            ELSE GREATEST(quantity * unit_price, 0)
        END as total_amount,
        invoice_date,
        batch_id,
        issue_type
    FROM sales_data
    """
    
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    
    # Map issue types to data quality keys
    df['data_quality_key'] = df['issue_type'].map(dq_keys)
    
    # Drop the issue_type column as it's not in the fact table
    df = df.drop('issue_type', axis=1)
    
    # Add audit columns
    now = datetime.now()
    df['created_at'] = now
    df['updated_at'] = now
    
    # Load to database
    df.to_sql('fact_sales', engine, schema='facts', if_exists='append', index=False)
    return len(df)

def main():
    """Main ETL process."""
    # Load environment variables
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', '.env')
    load_dotenv(env_path)
    
    # Get database connection
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    engine = create_engine(db_url)
    
    # Get date range from staging data
    date_query = """
    SELECT MIN(invoice_date)::date as min_date, 
           MAX(invoice_date)::date as max_date 
    FROM staging.stg_invoices
    """
    dates = pd.read_sql(date_query, engine)
    start_date = dates['min_date'].iloc[0]
    end_date = dates['max_date'].iloc[0]
    
    try:
        # Load dimensions first
        dates_loaded = populate_date_dimension(engine, start_date, end_date)
        print(f"Loaded {dates_loaded} dates into dim_date")
        
        customers_loaded = populate_customer_dimension(engine)
        print(f"Loaded {customers_loaded} customers into dim_customer")
        
        dq_loaded = populate_data_quality_dimension(engine)
        print(f"Loaded {dq_loaded} data quality rules")
        
        # Load facts
        sales_loaded = populate_fact_sales(engine)
        print(f"Loaded {sales_loaded} sales records into fact_sales")
        
        print("ETL process completed successfully!")
        
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        raise

if __name__ == '__main__':
    main()
