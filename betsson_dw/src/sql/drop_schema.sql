-- Drop tables in reverse order of creation
DROP TABLE IF EXISTS facts.fact_sales;

DROP TABLE IF EXISTS dimensions.dim_customer;
DROP TABLE IF EXISTS dimensions.dim_product;
DROP TABLE IF EXISTS dimensions.dim_date;

DROP TABLE IF EXISTS staging.stage_sales;

-- Drop schemas
DROP SCHEMA IF EXISTS facts CASCADE;
DROP SCHEMA IF EXISTS dimensions CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
