CREATE TABLE dim_date(
    date_id DATE PRIMARY KEY NOT NULL,
    year INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_name VARCHAR NOT NULL,
    month_name VARCHAR NOT NULL,
    quarter INT NOT NULL
);

CREATE TABLE dim_staff(
    staff_id INT PRIMARY KEY NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    department_name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    email_address varchar(319) NOT NULL
);

CREATE TABLE dim_location(
    location_id INT PRIMARY KEY NOT NULL,
    address_line_1 VARCHAR NOT NULL,
    address_line_2 VARCHAR,
    district VARCHAR,
    city VARCHAR NOT NULL,
    postal_code VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    phone VARCHAR NOT NULL
);

CREATE TABLE dim_currency(
    currency_id INT PRIMARY KEY NOT NULL,
    currency_code VARCHAR NOT NULL,
    currency_name VARCHAR NOT NULL
);

CREATE TABLE dim_design(
    design_id INT PRIMARY KEY NOT NULL,
    design_name VARCHAR NOT NULL,
    file_location VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL
);

CREATE TABLE dim_counterparty(
    counterparty_id INT PRIMARY KEY NOT NULL,
    counterparty_legal_name VARCHAR NOT NULL,
    counterparty_legal_address_line_1 VARCHAR  NOT NULL,
    counterparty_legal_address_line_2 VARCHAR  NOT NULL,
    counterparty_legal_district VARCHAR,
    counterparty_legal_city VARCHAR NOT NULL,
    counterparty_legal_postal_code VARCHAR NOT NULL,
    counterparty_legal_country VARCHAR NOT NULL,
    counterparty_legal_phone_number VARCHAR NOT NULL
);

CREATE TABLE dim_payment_type(
    payment_type_id INT PRIMARY KEY NOT NULL,
    payment_type_name VARCHAR NOT NULL
    );

CREATE TABLE dim_transaction(
    transaction_id INT PRIMARY KEY NOT NULL,
    transaction_type VARCHAR NOT NULL,
    sales_order_id INT,
    purchase_order_id INT
);



CREATE TABLE fact_sales_order(
    sales_record_id SERIAL PRIMARY KEY,
    sales_order_id INT NOT NULL,
    created_date DATE NOT NULL REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    sales_staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    units_sold INT NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    currency_id INT NOT NULL,
    design_id INT NOT NULL REFERENCES dim_design(design_id),
    agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT NOT NULL REFERENCES dim_location(location_id)
);

CREATE TABLE fact_purchase_order(
    purchase_record_id SERIAL PRIMARY KEY,
    purchase_order_id INT NOT NULL,
    created_date DATE NOT NULL REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    item_code VARCHAR NOT NULL,
    item_quantity INT NOT NULL,
    item_unit_price NUMERIC NOT NULL,
    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT NOT NULL
);

CREATE TABLE fact_payment(
    payment_record_id SERIAL PRIMARY KEY,
    payment_id INT NOT NULL,
    created_date DATE NOT NULL  REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL  REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    transaction_id INT NOT NULL REFERENCES dim_transaction(transaction_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    payment_amount NUMERIC NOT NULL,
    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    payment_type_id INT NOT NULL REFERENCES dim_payment_type(payment_type_id),
    paid BOOLEAN NOT NULL,
    payment_date DATE NOT NULL  REFERENCES dim_date(date_id)
);

