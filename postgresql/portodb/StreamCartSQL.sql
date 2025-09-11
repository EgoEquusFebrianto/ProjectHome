create schema stream_cart_schema;

set search_path to stream_cart_schema;
create extension if not exists "pgcrypto";

-- --------------------
-- OLTP Project Structure
-- --------------------

create table if not exists categories(
	category_id UUID primary key default gen_random_uuid(),
	category_name varchar(50) UNIQUE
);

CREATE TABLE if not exists products (
    product_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_name VARCHAR(100) NOT NULL,
    category_id UUID NOT NULL,
    price INTEGER NOT NULL,
    stock_quantity INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_products 
        FOREIGN KEY (category_id) 
        REFERENCES categories(category_id) 
        ON UPDATE CASCADE 
        ON DELETE CASCADE
);

create table if not exists customers(
	customer_id UUID primary key default gen_random_uuid(),
	username varchar(100) UNIQUE not null,
	email varchar(100) UNIQUE not null,
	created_at TIMESTAMPTZ default CURRENT_TIMESTAMP
);

create table if not exists orders(
	order_id UUID primary key default gen_random_uuid(),
	customer_id UUID not null,
	order_date TIMESTAMPTZ default CURRENT_TIMESTAMP,
	total_amount INTEGER not null,
	kafka_offset BIGINT,
	kafka_partition INTEGER,
	kafka_timestamp TIMESTAMPTZ,
	CONSTRAINT fk_orders
		foreign key(customer_id)
		references customers(customer_id)
		on update cascade
		on delete cascade
);

create table if not exists order_items(
	order_item_id UUID primary key default gen_random_uuid(),
	order_id UUID not null,
	product_id UUID not null,
	quantity INTEGER not null,
	unit_price INTEGER not null,
	sub_total INTEGER not null,
	CONSTRAINT order_items_orders
		foreign key(order_id)
		references orders(order_id)
		on update cascade
		on delete cascade,
	CONSTRAINT order_items_products
		foreign key(product_id)
		references products(product_id)
		on update cascade
		on delete cascade
);

create table if not exists event_logs(
	event_id UUID primary key default gen_random_uuid(),
	event_type varchar(50) not null,
	aggregate_id UUID not null,
	event_data JSONB not null,
	event_timestamp TIMESTAMPTZ default CURRENT_TIMESTAMP,
	kafka_topic varchar(50),
	kafka_offset BIGINT,
	kafka_partition INTEGER,
	processed BOOLEAN default false
);

-- --------------------
-- OLAP Data Structure
-- --------------------

create schema if not exists stream_cart_analytics;
set search_path to stream_cart_analytics;
create extension if not exists "pgcrypto";

-- tabel dimensi tanggal
create table if not exists dim_date(
	date_id DATE primary key,
	day INTEGER not null,
	month INTEGER not null,
	year INTEGER not null,
	quarter INTEGER not null,
	day_of_week INTEGER not null,
	is_weekend BOOLEAN not null
);

-- tabel dimensi penjualan berdasarkan produk, tanggal, kategori
create table if not exists fact_sales_summary(
	product_id UUID primary key not null,
	category_id UUID not null,
	sales_date Date not null,
	total_quantity_sold INTEGER not null,
	total_sales_amount INTEGER not null
);


-- tabel dimensi aktivitas dan pengeluaran tiap pelanggan
create table if not exists fact_costumer_sales(
	customer_id UUID primery key not null,
	sales_date DAte not null,
	total_orders INTEGER not null,
	total_spent INTEGER not null,
	average_order_valueINTEGER not null
);

-- tabel statistik harian
create table if not exists fact_order_daily(
	order_date DATE primary key,
	total_orders INTEGER not null,
	total_items_sold INTEGER not null,
	total_revenue INTEGER not nulll
);
