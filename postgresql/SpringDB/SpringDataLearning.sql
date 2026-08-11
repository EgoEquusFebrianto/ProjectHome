select current_database();
show search_path;
set search_path to spring_learning;

-- Demonstrasi

create table if not exists users(
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

INSERT INTO users(name,email) VALUES
	('Zeus','zeus@gmail.com'),
	('Hera','hera@gmail.com'),
	('Apollo','apollo@gmail.com');

select * from users;
drop table users;

-- ====================================
-- perpustakaan study case:
create table if not exists roles(
	id INTEGER PRIMARY KEY,
	role_name varchar(20) not null,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

create table if not exists users(
	id BIGSERIAL PRIMARY KEY,
	email VARCHAR(255) UNIQUE not null,
	password VARCHAR(255) not null,
	fullname VARCHAR(255) not null,
	phone VARCHAR(20) not null,
	role_id INTEGER not null default 1,
	status varchar(20) not null default 'ACTIVE',
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT fk_roles_role_id
		FOREIGN KEY(role_id)
		REFERENCES roles(id)
		ON UPDATE CASCADE
		ON DELETE CASCADE,
	Constraint length_password
		check( Length(password) >= 5)
);

CREATE TABLE books (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    author VARCHAR(255) NOT NULL,
    publisher VARCHAR(255),
    publication_year INTEGER,
    stock INTEGER DEFAULT 0
);

alter table books
	add column created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	add column updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

insert into roles values (1, 'CUSTOMER'), (2, 'ADMIN');
INSERT INTO books (title, author, publisher, publication_year, stock) VALUES
	('Clean Code: A Handbook of Agile Software Craftsmanship', 'Robert C. Martin', 'Prentice Hall', 2008, 5),
	('Atomic Habits: An Easy & Proven Way to Build Good Habits & Break Bad Ones', 'James Clear', 'Penguin Random House', 2018, 3),
	('The Pragmatic Programmer: Your Journey to Mastery', 'David Thomas, Andrew Hunt', 'Addison-Wesley', 1999, 2),
	('Sapiens: A Brief History of Humankind', 'Yuval Noah Harari', 'Harper', 2011, 4),
	('Harry Potter and the Sorcerer''s Stone', 'J.K. Rowling', 'Bloomsbury', 1997, 0);

select * from books;
select * from users;

-- ====================================
-- Retail Cases

create table IF NOT EXISTS sales(
	sale_id BIGSERIAL PRIMARY KEY,
	
)

-- Next Level Cases
CREATE TABLE if not exists categories(
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2),
    category_id INTEGER REFERENCES categories(id),
    stock_quantity INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert data categories
INSERT INTO categories (name, description) VALUES 
    ('Electronics', 'Electronic devices and gadgets'),
    ('Clothing', 'Apparel and fashion items'),
    ('Books', 'Books and publications'),
    ('Food', 'Food and beverages'),
    ('Furniture', 'Home and office furniture');

-- Insert data products
INSERT INTO products (name, description, price, category_id, stock_quantity) VALUES
    ('Samsung Galaxy S24', 'Latest Samsung smartphone with AI features', 999.99, 1, 50),
    ('Apple iPhone 15 Pro', 'Premium Apple smartphone', 1099.99, 1, 30),
    ('Sony WH-1000XM5', 'Noise cancelling headphones', 399.99, 1, 25),
    ('MacBook Pro 14', 'Apple laptop with M3 chip', 1999.99, 1, 15),
    ('Nike Air Max', 'Comfortable running shoes', 150.00, 2, 100),
    ('Levi''s Jeans 501', 'Classic blue jeans', 89.99, 2, 75),
    ('Adidas T-Shirt', 'Cotton sports t-shirt', 35.00, 2, 200),
    ('North Face Jacket', 'Waterproof winter jacket', 250.00, 2, 40),
    ('Clean Code', 'Programming best practices by Robert Martin', 45.99, 3, 60),
    ('The Pragmatic Programmer', 'Software development wisdom', 39.99, 3, 45),
    ('Sapiens', 'History of humankind by Yuval Harari', 25.00, 3, 80),
    ('Atomic Habits', 'Self-improvement and habits', 18.99, 3, 55),
    ('Organic Coffee Beans', 'Premium Arabica coffee', 15.99, 4, 120),
    ('Green Tea Matcha', 'Japanese ceremonial grade', 28.50, 4, 90),
    ('Dark Chocolate 70%', 'Belgian dark chocolate', 8.99, 4, 150),
    ('Honey Raw', 'Pure natural honey', 12.00, 4, 65),
    ('Office Chair Ergonomic', 'Adjustable office chair', 299.99, 5, 20),
    ('Wooden Desk', 'Solid oak desk 120cm', 450.00, 5, 10),
    ('Bookshelf 5-Tier', 'Industrial style bookshelf', 89.99, 5, 35),
    ('Floor Lamp', 'Modern LED floor lamp', 75.00, 5, 28);

-- Tambahkan beberapa produk dengan nama yang mirip untuk testing
INSERT INTO products (name, description, price, category_id, stock_quantity) VALUES
    ('Samsung Galaxy Watch', 'Smartwatch from Samsung', 299.99, 1, 30),
    ('Samsung Galaxy Buds', 'Wireless earbuds', 149.99, 1, 45),
    ('Apple AirPods Pro', 'Wireless earbuds from Apple', 249.99, 1, 40);

select p.*, c.name as category_name
from products p
left join categories c on p.category_id = c.id
where
	('book' is null or lower(p.name)like lower(concat('%', 'book', '%')))
and
	(null is null or p.category_id = 3);

select p.*
from products p
where lower(p.name) like '%book%'
order by
case
	when lower(p.name) like 'boo%' then 1
	else 0
end,
p.name;