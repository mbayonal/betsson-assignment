-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dimensions;
CREATE SCHEMA IF NOT EXISTS facts;

-- Create staging table
CREATE TABLE staging.stage_sales (
    stage_key SERIAL PRIMARY KEY,
    invoice VARCHAR(50) NOT NULL,
    stock_code VARCHAR(50) NOT NULL,
    description VARCHAR(500),
    quantity INTEGER NOT NULL,
    invoice_date VARCHAR(50) NOT NULL,
    price NUMERIC(10,2),
    customer_id VARCHAR(50),
    country VARCHAR(100),
    file_name VARCHAR(255) NOT NULL,
    load_date TIMESTAMP NOT NULL,
    is_processed BOOLEAN NOT NULL DEFAULT FALSE
);

-- Create indexes for staging table
CREATE INDEX idx_stage_sales_invoice ON staging.stage_sales(invoice);
CREATE INDEX idx_stage_sales_stock_code ON staging.stage_sales(stock_code);
CREATE INDEX idx_stage_sales_customer_id ON staging.stage_sales(customer_id);
CREATE INDEX idx_stage_sales_is_processed ON staging.stage_sales(is_processed);

-- Create dimension tables
CREATE TABLE dimensions.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_business_day BOOLEAN NOT NULL,
    fiscal_year INTEGER NOT NULL,
    fiscal_quarter INTEGER NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
);

-- Create indexes for date dimension
CREATE INDEX idx_dim_date_full_date ON dimensions.dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dimensions.dim_date(year, month);

CREATE TABLE dimensions.dim_product (
    product_key VARCHAR(100) PRIMARY KEY,
    stock_code VARCHAR(50) NOT NULL,
    description VARCHAR(500),
    unit_price NUMERIC(10,2) NOT NULL,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
);

-- Create indexes for product dimension
CREATE INDEX idx_dim_product_stock_code ON dimensions.dim_product(stock_code);
CREATE INDEX idx_dim_product_is_current ON dimensions.dim_product(is_current);

CREATE TABLE dimensions.dim_customer (
    customer_key VARCHAR(100) PRIMARY KEY,
    customer_id VARCHAR(50),
    country VARCHAR(100),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    is_current BOOLEAN NOT NULL
);

-- Create indexes for customer dimension
CREATE INDEX idx_dim_customer_customer_id ON dimensions.dim_customer(customer_id);
CREATE INDEX idx_dim_customer_is_current ON dimensions.dim_customer(is_current);

-- Create fact table
CREATE TABLE facts.fact_sales (
    fact_key SERIAL PRIMARY KEY,
    invoice_number VARCHAR(50) NOT NULL,
    date_key INTEGER REFERENCES dimensions.dim_date(date_key),
    product_key VARCHAR(100) REFERENCES dimensions.dim_product(product_key),
    customer_key VARCHAR(100) REFERENCES dimensions.dim_customer(customer_key),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP NOT NULL
);

-- Create indexes for fact table
CREATE INDEX idx_fact_sales_invoice_number ON facts.fact_sales(invoice_number);
CREATE INDEX idx_fact_sales_date_key ON facts.fact_sales(date_key);
CREATE INDEX idx_fact_sales_product_key ON facts.fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer_key ON facts.fact_sales(customer_key);
