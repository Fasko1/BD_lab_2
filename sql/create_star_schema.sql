DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customers CASCADE;
DROP TABLE IF EXISTS dim_sellers CASCADE;
DROP TABLE IF EXISTS dim_products CASCADE;
DROP TABLE IF EXISTS dim_stores CASCADE;
DROP TABLE IF EXISTS dim_suppliers CASCADE;
DROP TABLE IF EXISTS dim_dates CASCADE;

CREATE TABLE dim_customers (
    customer_key BIGINT PRIMARY KEY,
    sale_customer_id BIGINT,
    customer_first_name TEXT,
    customer_last_name TEXT,
    customer_age INT,
    customer_email TEXT,
    customer_country TEXT,
    customer_postal_code TEXT,
    customer_pet_type TEXT,
    customer_pet_name TEXT,
    customer_pet_breed TEXT
);

CREATE TABLE dim_sellers (
    seller_key BIGINT PRIMARY KEY,
    sale_seller_id BIGINT,
    seller_first_name TEXT,
    seller_last_name TEXT,
    seller_email TEXT,
    seller_country TEXT,
    seller_postal_code TEXT
);

CREATE TABLE dim_products (
    product_key BIGINT PRIMARY KEY,
    sale_product_id BIGINT,
    product_name TEXT,
    product_category TEXT,
    product_price NUMERIC(14,2),
    product_quantity INT,
    pet_category TEXT,
    product_weight NUMERIC(14,2),
    product_color TEXT,
    product_size TEXT,
    product_brand TEXT,
    product_material TEXT,
    product_description TEXT,
    product_rating NUMERIC(4,2),
    product_reviews INT,
    product_release_date DATE,
    product_expiry_date DATE
);

CREATE TABLE dim_stores (
    store_key BIGINT PRIMARY KEY,
    store_name TEXT,
    store_location TEXT,
    store_city TEXT,
    store_state TEXT,
    store_country TEXT,
    store_phone TEXT,
    store_email TEXT
);

CREATE TABLE dim_suppliers (
    supplier_key BIGINT PRIMARY KEY,
    supplier_name TEXT,
    supplier_contact TEXT,
    supplier_email TEXT,
    supplier_phone TEXT,
    supplier_address TEXT,
    supplier_city TEXT,
    supplier_country TEXT
);

CREATE TABLE dim_dates (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day_num INT,
    month_num INT,
    month_name TEXT,
    quarter_num INT,
    year_num INT
);

CREATE TABLE fact_sales (
    sale_key BIGINT PRIMARY KEY,
    source_sale_id BIGINT,
    date_key INT REFERENCES dim_dates(date_key),
    customer_key BIGINT REFERENCES dim_customers(customer_key),
    seller_key BIGINT REFERENCES dim_sellers(seller_key),
    product_key BIGINT REFERENCES dim_products(product_key),
    store_key BIGINT REFERENCES dim_stores(store_key),
    supplier_key BIGINT REFERENCES dim_suppliers(supplier_key),
    sale_quantity INT,
    sale_total_price NUMERIC(14,2)
);