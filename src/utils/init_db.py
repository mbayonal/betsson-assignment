import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

def init_database():
    """Initialize the database with the required schemas"""
    # Load environment variables from the config directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', '.env')
    load_dotenv(env_path)
    
    # Get database connection
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    engine = create_engine(db_url)
    
    # Create schemas
    with engine.connect() as conn:
        # Create schemas if they don't exist
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS dimensions;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS facts;"))
        conn.commit()
        
        # Set search path
        conn.execute(text("SET search_path TO staging, dimensions, facts, public;"))
        conn.commit()

if __name__ == '__main__':
    init_database()
    print("Database schemas initialized successfully")
