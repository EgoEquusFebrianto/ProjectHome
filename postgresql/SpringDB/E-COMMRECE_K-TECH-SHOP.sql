create schema if not exists retail_app_schema;

set search_path to retail_app_schema;
show search_path;

create table if not exists roles(
	id INTEGER PRIMARY KEY,
	role_name varchar(20) not null,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

create table if not exists users(
	id BIGSERIAL PRIMARY KEY,
	email VARCHAR(100) UNIQUE not null,
	password VARCHAR(100) UNIQUE not null,
	full_name VARCHAR(150) not null, 
	phone VARCHAR(20) not null,
	role_id INTEGER not null default 1,
	status varchar(20) not null default 'ACTIVE',
	avatar varchar(500) not null default 'profile/2026/07/default/customer1.jpg',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_roles_role_id
		FOREIGN KEY(role_id)
		REFERENCES roles(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
);

-- alter table users
-- 	alter column avatar type varchar(500),
-- 	alter column avatar set default 'profile/2026/07/default/customer1.jpg',
-- 	alter column avatar drop not null; 

create table if not exists categories(
	id BIGINT PRIMARY KEY,
	category_name VARCHAR(50) NOT NULL UNIQUE,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

create table if not exists products(
	id BIGSERIAL PRIMARY KEY,
	category_id BIGINT NOT NULL,
	product_name varchar(200) not null,
	price NUMERIC(15,2) NOT NULL,
	stock INTEGER NOT NULL,
	status VARCHAR(20) NOT NULL,
	storage_path varchar(500) not null,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_categories_category_id
		FOREIGN KEY(category_id)
		REFERENCES categories(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
);

-- create table if not exists product_images(
-- 	id BIGSERIAL PRIMARY KEY,
-- 	product_id BIGSERIAL NOT NULL,
-- 	storage_path varchar(500) not null,
-- 	display_order INTEGER NOT NULL DEFAULT 1,
-- 	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
-- 	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
-- 	CONSTRAINT fk_products_product_id
-- 		FOREIGN KEY(product_id)
-- 		REFERENCES products(id)
-- 		ON DELETE CASCADE
-- );


create table if not exists carts(
	id BIGSERIAL PRIMARY KEY,
	user_id BIGSERIAL NOT NULL,
	product_id BIGSERIAL NOT NULL,
	quantity INTEGER NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_users_user_id
		FOREIGN KEY(user_id)
		REFERENCES users(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE,
	CONSTRAINT fk_products_product_id
		FOREIGN KEY(product_id)
		REFERENCES products(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
);

create table if not exists orders(
	id BIGSERIAL PRIMARY KEY,
	user_id BIGSERIAL NOT NULL,
	order_number varchar(50) not null UNIQUE,
	total_amount NUMERIC(15,2) NOT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_orders_user
		FOREIGN KEY(user_id)
		REFERENCES users(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
);

create table if not exists order_items(
	id BIGSERIAL PRIMARY KEY,
	order_id BIGSERIAL NOT NULL,
	product_id BIGSERIAL NOT NULL,
	product_name varchar(200) NOT NULL,
	price NUMERIC(15,2) NOT NULL,
    quantity INTEGER NOT NULL,
	subtotal NUMERIC(15,2) NOT NULL,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_products_product_id
		FOREIGN KEY(product_id)
		REFERENCES products(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE,
	CONSTRAINT fk_orders_order_id
		FOREIGN KEY(order_id)
		REFERENCES orders(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE
);

create table if not exists product_summary(
	id INTEGER PRIMARY KEY,
	default_id BIGSERIAL,
	total_product INTEGER
);

-- TRIGGER
create or replace function product_summary_add_value()
returns trigger
language plpgsql
as $$
BEGIN
	update product_summary
	set total_product = total_product + 1
	where id = 1;

	return new;
END;
$$;

create or replace function product_summary_substract_value()
returns trigger
language plpgsql
as $$
BEGIN
	update product_summary
	set total_product = total_product - 1
	where id = 1;

	return old;
END;
$$;

create or replace trigger trg_add_product_summary
after insert on products
for each row
execute function product_summary_add_value();

create or replace trigger trg_substract_product_summary
after delete on products
for each row
execute function product_summary_substract_value();

-- INSERT DATA
insert into roles values (1, 'CUSTOMER'), (2, 'ADMIN');
insert into categories(id, name) values (572601, 'technology'), (572602, 'fashion');
insert into products(id, category_id, name, price, stock, status, storage_path) values
	(131211, 572601, 'IPhone', 999.0, 100, 'AVAILABLE', 'products/2026/07/1.png'),
	(131212, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE', 'products/2026/07/2.png'),
	(131213, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE', 'products/2026/07/3.png'),
	(131214, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE', 'products/2026/07/4.png'),
	(131215, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE', 'products/2026/07/5.png'),
	(131216, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE', 'products/2026/07/6.webp'),
	(131217, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE', 'products/2026/07/7.webp'),
	(131218, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE', 'products/2026/07/8.webp'),
	(131219, 572601, 'AMD Ryzen 5 7600X Processor', 229.99, 100, 'AVAILABLE', 'products/2026/07/amd-ryzen-5.webp'),
	(131220, 572601, 'LG UltraGear 24GS50F-B Monitor', 149.99, 100, 'AVAILABLE', 'products/2026/07/monitor-1.webp'),
	(131221, 572601, 'Xiaomi MiFa A1 Bluetooth Speaker', 29.99, 100, 'AVAILABLE', 'products/2026/07/speaker-1.webp'),
	(131222, 572601, 'Logitech M840L Silent Mouse', 39.99, 100, 'AVAILABLE', 'products/2026/07/mouse-1.webp'),
	(131223, 572602, 'Women''s Cotton Round Neck Top', 25.99, 100, 'AVAILABLE', 'products/2026/07/t-shrt-1.webp'),
	(131224, 572602, 'Versace Medusa BNW T-Shirt', 185.00, 100, 'AVAILABLE', 'products/2026/07/t-shrt-2.webp'),
	(131225, 572602, 'Garmin Fenix 8 Titanium Watch', 999.99, 100, 'AVAILABLE', 'products/2026/07/watch-1.webp'),
	(131226, 572602, 'On Cloudpulse Next Men Shoes', 139.99, 100, 'AVAILABLE', 'products/2026/07/shoes-1.webp');

-- duplicated for testing
insert into products(id, category_id, name, price, stock, status, storage_path) values
	(131227, 572601, 'IPhone', 999.0, 100, 'AVAILABLE', 'products/2026/07/1.png'),
	(131228, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE', 'products/2026/07/2.png'),
	(131229, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE', 'products/2026/07/3.png'),
	(131230, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE', 'products/2026/07/4.png'),
	(131231, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE', 'products/2026/07/5.png'),
	(131232, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE', 'products/2026/07/6.webp'),
	(131233, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE', 'products/2026/07/7.webp'),
	(131234, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE', 'products/2026/07/8.webp'),
	(131235, 572601, 'AMD Ryzen 5 7600X Processor', 229.99, 100, 'AVAILABLE', 'products/2026/07/amd-ryzen-5.webp'),
	(131236, 572601, 'LG UltraGear 24GS50F-B Monitor', 149.99, 100, 'AVAILABLE', 'products/2026/07/monitor-1.webp'),
	(131237, 572601, 'Xiaomi MiFa A1 Bluetooth Speaker', 29.99, 100, 'AVAILABLE', 'products/2026/07/speaker-1.webp'),
	(131238, 572601, 'Logitech M840L Silent Mouse', 39.99, 100, 'AVAILABLE', 'products/2026/07/mouse-1.webp'),
	(131239, 572602, 'Women''s Cotton Round Neck Top', 25.99, 100, 'AVAILABLE', 'products/2026/07/t-shrt-1.webp'),
	(131240, 572602, 'Versace Medusa BNW T-Shirt', 185.00, 100, 'AVAILABLE', 'products/2026/07/t-shrt-2.webp'),
	(131241, 572602, 'Garmin Fenix 8 Titanium Watch', 999.99, 100, 'AVAILABLE', 'products/2026/07/watch-1.webp'),
	(131242, 572602, 'On Cloudpulse Next Men Shoes', 139.99, 100, 'AVAILABLE', 'products/2026/07/shoes-1.webp'),
	(131243, 572601, 'IPhone', 999.0, 100, 'AVAILABLE', 'products/2026/07/1.png'),
	(131244, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE', 'products/2026/07/2.png'),
	(131245, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE', 'products/2026/07/3.png'),
	(131246, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE', 'products/2026/07/4.png'),
	(131247, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE', 'products/2026/07/5.png'),
	(131248, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE', 'products/2026/07/6.webp'),
	(131249, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE', 'products/2026/07/7.webp'),
	(131250, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE', 'products/2026/07/8.webp'),
	(131251, 572601, 'AMD Ryzen 5 7600X Processor', 229.99, 100, 'AVAILABLE', 'products/2026/07/amd-ryzen-5.webp'),
	(131252, 572601, 'LG UltraGear 24GS50F-B Monitor', 149.99, 100, 'AVAILABLE', 'products/2026/07/monitor-1.webp'),
	(131253, 572601, 'Xiaomi MiFa A1 Bluetooth Speaker', 29.99, 100, 'AVAILABLE', 'products/2026/07/speaker-1.webp'),
	(131254, 572601, 'Logitech M840L Silent Mouse', 39.99, 100, 'AVAILABLE', 'products/2026/07/mouse-1.webp'),
	(131255, 572602, 'Women''s Cotton Round Neck Top', 25.99, 100, 'AVAILABLE', 'products/2026/07/t-shrt-1.webp'),
	(131256, 572602, 'Versace Medusa BNW T-Shirt', 185.00, 100, 'AVAILABLE', 'products/2026/07/t-shrt-2.webp'),
	(131257, 572602, 'Garmin Fenix 8 Titanium Watch', 999.99, 100, 'AVAILABLE', 'products/2026/07/watch-1.webp'),
	(131258, 572602, 'On Cloudpulse Next Men Shoes', 139.99, 100, 'AVAILABLE', 'products/2026/07/shoes-1.webp'),
	(131259, 572601, 'IPhone', 999.0, 100, 'AVAILABLE', 'products/2026/07/1.png'),
	(131260, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE', 'products/2026/07/2.png'),
	(131261, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE', 'products/2026/07/3.png'),
	(131262, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE', 'products/2026/07/4.png'),
	(131263, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE', 'products/2026/07/5.png'),
	(131264, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE', 'products/2026/07/6.webp'),
	(131265, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE', 'products/2026/07/7.webp'),
	(131266, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE', 'products/2026/07/8.webp'),
	(131267, 572601, 'AMD Ryzen 5 7600X Processor', 229.99, 100, 'AVAILABLE', 'products/2026/07/amd-ryzen-5.webp'),
	(131268, 572601, 'LG UltraGear 24GS50F-B Monitor', 149.99, 100, 'AVAILABLE', 'products/2026/07/monitor-1.webp'),
	(131269, 572601, 'Xiaomi MiFa A1 Bluetooth Speaker', 29.99, 100, 'AVAILABLE', 'products/2026/07/speaker-1.webp'),
	(131270, 572601, 'Logitech M840L Silent Mouse', 39.99, 100, 'AVAILABLE', 'products/2026/07/mouse-1.webp'),
	(131271, 572602, 'Women''s Cotton Round Neck Top', 25.99, 100, 'AVAILABLE', 'products/2026/07/t-shrt-1.webp'),
	(131272, 572602, 'Versace Medusa BNW T-Shirt', 185.00, 100, 'AVAILABLE', 'products/2026/07/t-shrt-2.webp'),
	(131273, 572602, 'Garmin Fenix 8 Titanium Watch', 999.99, 100, 'AVAILABLE', 'products/2026/07/watch-1.webp'),
	(131274, 572602, 'On Cloudpulse Next Men Shoes', 139.99, 100, 'AVAILABLE', 'products/2026/07/shoes-1.webp'),
	(131275, 572601, 'IPhone', 999.0, 100, 'AVAILABLE', 'products/2026/07/1.png'),
	(131276, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE', 'products/2026/07/2.png'),
	(131277, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE', 'products/2026/07/3.png'),
	(131278, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE', 'products/2026/07/4.png'),
	(131279, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE', 'products/2026/07/5.png'),
	(131280, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE', 'products/2026/07/6.webp'),
	(131281, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE', 'products/2026/07/7.webp'),
	(131282, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE', 'products/2026/07/8.webp'),
	(131283, 572601, 'AMD Ryzen 5 7600X Processor', 229.99, 100, 'AVAILABLE', 'products/2026/07/amd-ryzen-5.webp'),
	(131284, 572601, 'LG UltraGear 24GS50F-B Monitor', 149.99, 100, 'AVAILABLE', 'products/2026/07/monitor-1.webp'),
	(131285, 572601, 'Xiaomi MiFa A1 Bluetooth Speaker', 29.99, 100, 'AVAILABLE', 'products/2026/07/speaker-1.webp'),
	(131286, 572601, 'Logitech M840L Silent Mouse', 39.99, 100, 'AVAILABLE', 'products/2026/07/mouse-1.webp'),
	(131287, 572602, 'Women''s Cotton Round Neck Top', 25.99, 100, 'AVAILABLE', 'products/2026/07/t-shrt-1.webp'),
	(131288, 572602, 'Versace Medusa BNW T-Shirt', 185.00, 100, 'AVAILABLE', 'products/2026/07/t-shrt-2.webp'),
	(131289, 572602, 'Garmin Fenix 8 Titanium Watch', 999.99, 100, 'AVAILABLE', 'products/2026/07/watch-1.webp'),
	(131290, 572602, 'On Cloudpulse Next Men Shoes', 139.99, 100, 'AVAILABLE', 'products/2026/07/shoes-1.webp');
	
insert into product_summary values (1, 131210, 8);
insert into users(id, email, password, fullname, phone, role_id, avatar) values
	(
		1, 
		'customer1@gmail.com', 
		'$2a$10$uCQXvfJmdUdiOn/f7JkbiO1npogn7VBu4VC3JLYFv4CjhKcggtKSK', -- plaintext: customer123
		'John Doe', 
		'+628123456789',
		1,
		'profile/2026/07/default/customer1.jpg'
	);

select * from roles;
select * from users;
select * from products;
select * from categories;
select * from product_summary;
select * from carts;
select * from orders;
select * from order_items;
select * from personal_access_tokens;
select * from products where name ilike 'led%';

truncate table orders cascade;
truncate table users cascade;

-- select * from product_images;

-- insert into products(id, category_id, name, price, stock, status) values
-- 	(131211, 572601, 'IPhone', 999.0, 100, 'AVAILABLE'),
-- 	(131212, 572601, 'Macbook Pro 2022 (M1)', 1999.0, 100, 'AVAILABLE'),
-- 	(131213, 572601, 'Cannon M50 Camera', 699.0, 100, 'AVAILABLE'),
-- 	(131214, 572601, 'WLS Van Gogh Denim Jacket', 228.0, 100, 'AVAILABLE'),
-- 	(131215, 572601, 'LED Light Strips', 19.99, 100, 'AVAILABLE'),
-- 	(131216, 572602, 'SPECTRUM LS TEE', 68.0, 100, 'AVAILABLE'),
-- 	(131217, 572602, 'AUTO SERVICE SHIRT by GOLF WANG', 120.0, 100, 'AVAILABLE'),
-- 	(131218, 572602, 'DON''T TRIP UNSTRUCTURED HAT', 40.0, 100, 'AVAILABLE');
	
-- insert into product_images values
-- 	(1, 131211, 'products/2026/07/1.png'),
-- 	(2, 131212, 'products/2026/07/2.png'),
-- 	(3, 131213, 'products/2026/07/3.png'),
-- 	(4, 131214, 'products/2026/07/4.png'),
-- 	(5, 131215, 'products/2026/07/5.png'),
-- 	(6, 131216, 'products/2026/07/6.webp'),
-- 	(7, 131217, 'products/2026/07/7.webp'),
-- 	(8, 131218, 'products/2026/07/8.webp');