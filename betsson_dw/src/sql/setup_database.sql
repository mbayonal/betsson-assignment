-- Create database user
CREATE USER betsson_user WITH PASSWORD 'secure_password';

-- Create database
CREATE DATABASE betsson_dw OWNER betsson_user;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE betsson_dw TO betsson_user;
