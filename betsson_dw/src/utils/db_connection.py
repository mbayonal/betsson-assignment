from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv('config/.env')

def get_connection_string():
    """Create database connection string from environment variables."""
    return (f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

def create_db_engine():
    """Create and return SQLAlchemy engine."""
    return create_engine(get_connection_string())

def get_db_session():
    """Create and return a database session."""
    engine = create_db_engine()
    Session = sessionmaker(bind=engine)
    return Session()
