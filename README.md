# Data Warehouse POC

This project implements a data warehouse following Kimball's dimensional modeling architecture.

## Project Structure
```
├── src/
│   ├── staging/         # Staging area for raw data
│   ├── dimensions/      # Dimension tables implementation
│   ├── facts/          # Fact tables implementation
│   ├── etl/            # ETL processes
│   └── utils/          # Utility functions
├── config/             # Configuration files
├── tests/              # Unit tests
└── data/              # Sample data files
```

## Setup
1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure database connection in `config/.env`

## Project Components
- Staging Layer: Initial data landing
- Dimension Layer: SCD (Slowly Changing Dimensions) handling
- Fact Tables: Business process measurements
- ETL Pipeline: Data transformation and loading processes
