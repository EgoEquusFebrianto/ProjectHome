select current_database();
show search_path;
select table_name from information_schema.tables where table_schema = 'public';

-- XX QUERY DASAR XX

-- create table

create table customers(
	customerId varchar(5),
	customerName varchar(50) not null,
	genre varchar(1) not null check (genre in ('M', 'F')),
	address varchar(50) not null,
	city varchar(50) not null,
	region varchar(50) not null default '-',
	country varchar(50) not null,
	zip varchar(5) not null default '-',
	phone varchar(20) not null default '+62',
	CONSTRAINT PK_customers PRIMARY KEY (customerID)
);

create table products(
	productId varchar(5),
	productName varchar(50) not null,
	unitPrice decimal not null default 0,
	unitStock integer not null default 0,
	unitOrder integer not null default 0,
	CONSTRAINT PK_products PRIMARY KEY (productId)
);

create table orders(
	orderId varchar(5),
	customerId varchar(5) not null,
	orderDate timestamp not null default current_timestamp,
	CONSTRAINT PK_orders PRIMARY KEY (orderId),
	CONSTRAINT FK_orders FOREIGN KEY (customerId) references customers(customerId)
		on update cascade
		on delete cascade
);

create table order_details(
	orderId varchar(5) not null,
	productId varchar(5) not null,
	unitPrice decimal not null default 0,
	quantity integer not null default 0
);

-- alter table

alter table order_details
	add discount REAL not null default 0;

alter table customers
	alter column country type varchar(50), 
	alter column country set not null;

alter table order_details
	add column subTotal real generated always as (unitPrice * quantity * (1 - discount)) stored;

alter table order_details
	drop column subTotal;

alter table order_details
	add constraint PK_order_detail PRIMARY KEY (orderId, productId);

alter table order_details
	add constraint FK_order_detail FOREIGN KEY (orderId)
	references orders(orderId) on update cascade on delete cascade;

alter table order_details
	add constraint FK_order_detail_productId FOREIGN KEY (productId)
	references products(productId) on update cascade on delete cascade;

--drop table
drop table customers;

-- insert table

insert into products values 
	('A0001', 'Keyboard', 15000, 25, 0),
	('A0002', 'Mouse', 10000, 20, 10 );

insert into products(productId, productName, unitPrice) values
	('A0003', 'Mouse Pad', 5000);

INSERT INTO products( productId, productName, unitStock) VALUES
	('A0004', 'Monitor', 10 );

INSERT INTO products( productId, productName, unitPrice ) VALUES
	('A0005', 'Speaker', 150000 ),
	('A0006', 'Casing', 200000 );

-- update
update products
	set unitPrice = 25000
	where productId = 'A0002';

update products
	set productName = 'Mouse', unitStock = 25
	where productId = 'A0002';

update products
	set unitOrder= 0
	where unitPrice > 10000 and unitStock > 20;

update products
	set unitPrice = default, unitStock = default, unitOrder = default
	where productId = 'A0002';

-- delete
delete from products
	where productid = 'A0002';

delete from products; -- beware with this.. 



select * from products;