import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.utils.metadata import metadata
from src.dimensions.dim_customer import DimCustomer
from src.dimensions.dim_date import DimDate
from src.dimensions.dim_data_quality import DimDataQuality
from src.facts.fact_sales import FactSales

def setup_warehouse():
    """Set up the data warehouse schemas and tables."""
    # Load environment variables
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', '.env')
    load_dotenv(env_path)
    
    # Get database connection
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    engine = create_engine(db_url)
    
    # Drop existing tables
    print("Dropping existing tables...")
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS dimensions CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS facts CASCADE"))
        conn.commit()
    
    # Create schemas
    print("Creating schemas...")
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensions"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS facts"))
        conn.commit()
    
    # Create tables
    print("Creating tables...")
    metadata.create_all(engine, tables=[
        DimCustomer.__table__,
        DimDate.__table__,
        DimDataQuality.__table__,
        FactSales.__table__
    ])
    
    print("Data warehouse tables created successfully")

if __name__ == '__main__':
    setup_warehouse()
