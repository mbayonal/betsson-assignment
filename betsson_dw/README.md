# Betsson Data Warehouse

A modular data warehouse implementation for processing sales data from CSV sources, specifically designed to handle the `Invoices_Year_2009-2010.csv` dataset.

## Project Overview

This data warehouse implements a star schema optimized for analyzing sales data. The ETL pipeline extracts data from CSV files, transforms it according to the dimensional model, and loads it into PostgreSQL database tables.

## Data Model

### Source Data
The warehouse processes data from `Invoices_Year_2009-2010.csv` with the following structure:
- **Invoice** (String): Transaction identifier
- **StockCode** (String): Product identifier
- **Description** (String): Product description
- **Quantity** (Integer): Units sold
- **InvoiceDate** (String): Transaction date/time in MM/DD/YYYY HH:MM format
- **Price** (Float): Unit price
- **Customer ID** (String): Customer identifier (nullable)
- **Country** (String): Order country

### Staging Layer
- **stage_sales**: Direct mapping of source CSV data with additional ETL tracking fields
  - Primary source fields: invoice, stock_code, description, quantity, invoice_date, price, customer_id, country
  - ETL tracking: file_name, load_date, is_processed

### Dimension Tables
1. **DimDate**
   - **date_key** (Integer, PK): Date in YYYYMMDD format
   - **full_date** (Date): Complete date
   - Time components: year, month, day, hour, minute
   - Calendar attributes: day_of_week, day_name, month_name, quarter
   - Business calendar: is_weekend, is_business_day, fiscal_year, fiscal_quarter
   - SCD Type 2 tracking: valid_from, valid_to, is_current

2. **DimProduct**
   - **product_key** (String, PK): Surrogate key with stock_code and timestamp
   - **stock_code** (String): Business key from source
   - **description** (String): Product description
   - **unit_price** (Numeric): Default price of product
   - SCD Type 2 tracking: valid_from, valid_to, is_current

3. **DimCustomer**
   - **customer_key** (String, PK): Surrogate key
   - **customer_id** (String, nullable): Business key from source
   - **country** (String): Customer's country
   - SCD Type 2 tracking: valid_from, valid_to, is_current
   - Special record: "ANONYMOUS" for null customer_id values

### Fact Table
- **FactSales**
  - **fact_key** (Integer, PK): Auto-incrementing surrogate key
  - **invoice_number** (String): Business key from source
  - Foreign keys: date_key, product_key, customer_key
  - Measures: quantity, unit_price, total_amount
  - Audit: created_at

## ETL Pipeline

The ETL pipeline consists of four main components:

### 1. Extraction
- **CsvExtractor**: Reads data from CSV files with proper encoding (ISO-8859-1)
- Maps CSV columns directly to staging table fields
- Adds ETL tracking metadata (file_name, load_date, is_processed)
- Supports batch processing for performance

### 2. Transformation
The transformation process is divided into four sequential steps:

1. **DateTransformer**
   - Extracts unique dates from staging table
   - Parses dates from MM/DD/YYYY HH:MM format
   - Creates calendar attributes (day of week, month name, etc.)
   - Calculates fiscal periods
   - Implements SCD Type 2 for changes

2. **ProductTransformer**
   - Extracts unique products from staging table
   - Creates surrogate keys combining stock_code and timestamp
   - Handles NULL values in unit_price (defaults to 0.0)
   - Implements SCD Type 2 for product changes

3. **CustomerTransformer**
   - Extracts unique customers from staging table
   - Creates surrogate keys for each customer
   - Creates special "ANONYMOUS" record for NULL customer_id values
   - Implements SCD Type 2 for customer changes

4. **SalesTransformer**
   - Links sales records to dimension tables via surrogate keys
   - Calculates total_amount from quantity and unit_price
   - Handles foreign key constraints
   - Marks staging records as processed
   - Implements batch processing and error handling

### 3. Loading
- Each transformer loads data directly into its respective dimension or fact table
- Uses SQLAlchemy ORM for most operations
- Uses direct SQL for complex operations or performance optimization

### 4. Orchestration
- Each transformation step can be run independently via dedicated scripts:
  - `run_transform_date.py`
  - `run_transform_product.py`
  - `run_transform_customer.py`
  - `run_transform_sales.py`
- Proper sequencing ensures referential integrity

## Project Structure

```
betsson_dw/
├── config/              # Configuration files and environment variables
├── data/                # Source data files (CSV)
├── exports/             # Exported data samples and reports
├── docs/                # Documentation
├── src/                 # Source code
│   ├── core/            # Core data warehouse components
│   │   ├── dimensions/  # Dimension tables
│   │   ├── facts/       # Fact tables
│   │   └── staging/     # Staging tables
│   ├── etl/             # ETL pipeline components
│   │   ├── extract/     # Data extraction modules
│   │   ├── transform/   # Data transformation modules
│   │   └── load/        # Data loading modules
│   ├── governance/      # Data governance and quality
│   ├── models/          # Data models and schemas
│   └── utils/           # Utility functions and helpers
└── tests/               # Test suite
```

## Installation

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
```

2. Install the package:
```bash
pip install -e .
```

3. Configure environment variables in `config/.env`:
```env
DB_USER=betsson_user
DB_PASSWORD=betsson_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=betsson_dw
```

## Usage

### Running the ETL Pipeline

1. Extract data from CSV to staging:
```bash
python run_extract.py --file data/Invoices_Year_2009-2010.csv
```

2. Transform date dimension:
```bash
python run_transform_date.py --verbose
```

3. Transform product dimension:
```bash
python run_transform_product.py --verbose
```

4. Transform customer dimension:
```bash
python run_transform_customer.py --verbose
```

5. Transform sales facts:
```bash
python run_transform_sales.py --verbose
```

### Sample Queries

After loading the data, you can run analytical queries such as:

1. Monthly sales:
```sql
SELECT d.month_name, d.year, SUM(f.total_amount) as monthly_sales 
FROM facts.fact_sales f 
JOIN dimensions.dim_date d ON f.date_key = d.date_key 
GROUP BY d.month_name, d.year, d.month 
ORDER BY d.year, d.month;
```

2. Sales by country:
```sql
SELECT c.country, COUNT(DISTINCT f.invoice_number) as order_count, 
       SUM(f.total_amount) as total_sales 
FROM facts.fact_sales f 
JOIN dimensions.dim_customer c ON f.customer_key = c.customer_key 
GROUP BY c.country 
ORDER BY total_sales DESC;
```

3. Top products:
```sql
SELECT p.stock_code, p.description, SUM(f.quantity) as total_quantity, 
       SUM(f.total_amount) as total_sales 
FROM facts.fact_sales f 
JOIN dimensions.dim_product p ON f.product_key = p.product_key 
GROUP BY p.stock_code, p.description 
ORDER BY total_sales DESC;
```

## Data Quality Features

1. **Error Handling**
   - Comprehensive exception handling in all transformers
   - Transaction rollback on failure
   - Detailed error logging

2. **Data Validation**
   - Type checking and conversion
   - NULL value handling
   - Default values for missing data

3. **Referential Integrity**
   - Foreign key constraints
   - "ANONYMOUS" customer for NULL customer_id values
   - Proper dimension lookups before fact table insertion

4. **SCD Type 2**
   - Proper tracking of dimension changes
   - valid_from/valid_to date ranges
   - is_current flag for current version

## Development

1. Install development dependencies:
```bash
pip install -r requirements-dev.txt
```

2. Run tests:
```bash
pytest tests/
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests
4. Submit a pull request
