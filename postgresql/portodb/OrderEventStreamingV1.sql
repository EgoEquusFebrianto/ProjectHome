create schema order_event_streaming_v1;
set search_path to order_event_streaming_v1;
show search_path;

--- OLTP Data Structure
create table customers(
	customer_id varchar(30) not null primary key,
	customer_name varchar(90) not null,
	email varchar(90) not null,
	country varchar(60) not null
);

create table categories(
	category_id varchar(10) not null primary key,
	category_name varchar(30) not null
);

create table payment_types(
	payment_type_id varchar(20) not null primary key,
	payment_name varchar(30) not null
);

create table transactions(
	transaction_id varchar(20) not null primary key
);

create table products(
	product_id varchar(30) not null primary key,
	product_name varchar(60) not null,
	category_id varchar(10) not null,
	constraint products_categories_id
		foreign key(category_id)
		references categories(category_id)
			on update cascade
			on delete restrict
);

create table transaction_items(
	transaction_item_id uuid not null primary key default gen_random_uuid(),
	transaction_id varchar(20) not null,
	product_id varchar(30) not null,
	unit_price decimal(15,2) not null,
	qty int not null,
	constraint transaction_item_products_id
		foreign key(product_id)
		references products(product_id)
			on update cascade
			on delete restrict,
	constraint transaction_item_transactions_id
		foreign key(transaction_id)
		references transactions(transaction_id)
			on update cascade
			on delete restrict
);

create table orders(
	order_id varchar(30) not null primary key,
	customer_id varchar(30) not null,
	transaction_id varchar(20) not null,
	order_timestamp timestamp not null,
	status varchar(30) not null,
	total_amount decimal(15,2) not null,
	constraint orders_customers_id
		foreign key(customer_id)
		references customers(customer_id)
			on update cascade
			on delete restrict,
	constraint orders_transactions_id
		foreign key(transaction_id)
		references transactions(transaction_id)
			on update cascade
			on delete restrict
);

create table order_payments(
	order_id varchar(30) not null,
	payment_type_id varchar(20) not null,
	payment_status BOOLEAN not null,
	constraint order_payment_payment_types_id
		foreign key (payment_type_id)
		references payment_types(payment_type_id)
			on update cascade
			on delete restrict,
	constraint order_payment_orders_id 
		foreign key(order_id) 
		references orders(order_id) 
			on update cascade 
			on delete restrict
);

create table raw_data(
	event_id varchar(60) not null primary key,
	event_timestamp timestamp not null,
	order_id varchar(30) not null,
	customer_id varchar(30) not null,
	constraint raw_data_order_id 
		foreign key(order_id) 
		references orders(order_id) 
			on update cascade 
			on delete restrict,
	constraint raw_data_customers_id 
		foreign key(customer_id) 
		references customers(customer_id) 
			on update cascade 
			on delete restrict
);

insert into categories values
	('CATEGORY01', 'Electronics'),
	('CATEGORY02', 'Fashion'),
	('CATEGORY03', 'Sports'),
	('CATEGORY04', 'Home'),
	('CATEGORY05', 'Books & Stationery');

insert into products values
	('PROD_ELEC_01', 'Smartphone', 'CATEGORY01'),
	('PROD_ELEC_02', 'Laptop', 'CATEGORY01'),
	('PROD_ELEC_03', 'Wireless Headphones', 'CATEGORY01'),
	('PROD_ELEC_04', 'Smart Watch', 'CATEGORY01'),
	('PROD_ELEC_05', 'Tablet', 'CATEGORY01'),
	('PROD_ELEC_06', 'Bluetooth Speaker', 'CATEGORY01'),
	('PROD_ELEC_07', 'Gaming Console', 'CATEGORY01'),
	('PROD_FASH_01', 'T-Shirt', 'CATEGORY02'),
	('PROD_FASH_02', 'Jeans', 'CATEGORY02'),
	('PROD_FASH_03', 'Running Shoes', 'CATEGORY02'),
	('PROD_FASH_04', 'Winter Jacket', 'CATEGORY02'),
	('PROD_FASH_05', 'Sunglasses', 'CATEGORY02'),
	('PROD_FASH_06', 'Backpack', 'CATEGORY02'),
	('PROD_FASH_07', 'Dress', 'CATEGORY02'),
	('PROD_SPORT_01', 'Basketball', 'CATEGORY03'),
	('PROD_SPORT_02', 'Yoga Mat', 'CATEGORY03'),
	('PROD_SPORT_03', 'Running Shoes', 'CATEGORY03'),
	('PROD_SPORT_04', 'Dumbbells Set', 'CATEGORY03'),
	('PROD_SPORT_05', 'Tennis Racket', 'CATEGORY03'),
	('PROD_SPORT_06', 'Swimming Goggles', 'CATEGORY03'),
	('PROD_SPORT_07', 'Fitness Tracker', 'CATEGORY03'),
	('PROD_HOME_01', 'Coffee Maker', 'CATEGORY04'),
	('PROD_HOME_02', 'Air Fryer', 'CATEGORY04'),
	('PROD_HOME_03', 'Vacuum Cleaner', 'CATEGORY04'),
	('PROD_HOME_04', 'Bedding Set', 'CATEGORY04'),
	('PROD_HOME_05', 'Kitchen Knife Set', 'CATEGORY04'),
	('PROD_HOME_06', 'LED Lamp', 'CATEGORY04'),
	('PROD_HOME_07', 'Blender', 'CATEGORY04'),
	('PROD_BOOK_01', 'Novel', 'CATEGORY05'),
	('PROD_BOOK_02', 'Notebook', 'CATEGORY05'),
	('PROD_BOOK_03', 'Fountain Pen', 'CATEGORY05'),
	('PROD_BOOK_04', 'Desk Organizer', 'CATEGORY05'),
	('PROD_BOOK_05', 'Sketchbook', 'CATEGORY05'),
	('PROD_BOOK_06', 'Bookmark Set', 'CATEGORY05'),
	('PROD_BOOK_07', 'Sticky Notes', 'CATEGORY05');
	
select * from products;