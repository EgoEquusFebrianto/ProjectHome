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
CREATE TABLE books (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    author VARCHAR(100) NOT NULL,
    publisher VARCHAR(100),
    publication_year INTEGER,
    stock INTEGER DEFAULT 0
);

INSERT INTO books (title, author, publisher, publication_year, stock) VALUES
	('Clean Code: A Handbook of Agile Software Craftsmanship', 'Robert C. Martin', 'Prentice Hall', 2008, 5),
	('Atomic Habits: An Easy & Proven Way to Build Good Habits & Break Bad Ones', 'James Clear', 'Penguin Random House', 2018, 3),
	('The Pragmatic Programmer: Your Journey to Mastery', 'David Thomas, Andrew Hunt', 'Addison-Wesley', 1999, 2),
	('Sapiens: A Brief History of Humankind', 'Yuval Noah Harari', 'Harper', 2011, 4),
	('Harry Potter and the Sorcerer''s Stone', 'J.K. Rowling', 'Bloomsbury', 1997, 0);

select * from books;

-- ====================================
-- Retail Cases

create table if not exists sales(
	sale_id BIGSERIAL PRIMARY KEY,
	
)