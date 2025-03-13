from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Import our models
from ..dimensions.dim_base import Base as DimBase
from ..dimensions.dim_customer import DimCustomer
from ..dimensions.dim_date import DimDate
from ..dimensions.dim_product import DimProduct

# Import fact tables
from ..facts.fact_sales import Base as SalesBase, FactSales

from ..staging.stage_customer import Base as StageBase, StageCustomer
from ..data_governance.data_quality_metrics import Base as QualityBase
from ..data_governance.data_profiling import Base as ProfilingBase

def create_database():
    """Create the database if it doesn't exist."""
    # Load environment variables
    load_dotenv('config/.env')
    
    # Connect to default PostgreSQL database
    default_conn_string = (f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                         f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/postgres")
    
    engine = create_engine(default_conn_string)
    
    # Create database if it doesn't exist
    with engine.connect() as conn:
        conn.execute(text("COMMIT"))  # Close any open transactions
        conn.execute(text(f"DROP DATABASE IF EXISTS {os.getenv('DB_NAME')}"))
        conn.execute(text(f"CREATE DATABASE {os.getenv('DB_NAME')}"))

def init_schema():
    """Initialize the database schema."""
    from ..utils.db_connection import create_db_engine
    
    engine = create_db_engine()
    
    # Create schemas
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensions"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS facts"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS data_governance"))
        conn.execute(text("COMMIT"))
    
    # Create all tables
    StageBase.metadata.create_all(engine)
    DimBase.metadata.create_all(engine)
    SalesBase.metadata.create_all(engine)
    QualityBase.metadata.create_all(engine)
    ProfilingBase.metadata.create_all(engine)
    
    print("Database schema created successfully!")

if __name__ == "__main__":
    create_database()
    init_schema()
