select current_database();
show search_path;
set search_path to "example_company";
select table_name from information_schema.tables where table_schema = 'example_company';

-- Data Definition Language (DDL)
-- Target: Database, Schema, Table, Constraint, View, Index

-- DDL Operation: Create, Delete(Drop), Update, Rename, ..

create DATABASE <db_name>;
create SCHEMA "postgres.example_company";
drop schema "postgres.example_company";
create schema example_company;

create table if not exists departments(
	department_id varchar(4) primary key,
	department_name varchar(50) not null,
	department_employees INTEGER not null
);

create table if not exists employees(
	employee_id varchar(4) primary key,
	department_id varchar(4) not null,
	name varchar(50) not null,
	email varchar(50) not null,
	gender varchar(9) not null check (gender in ('laki-laki', 'perempuan', 'LAKI-LAKI', 'PEREMPUAN')),
	salary integer not null,
	constraint fk_department foreign key(department_id) references departments(department_id)
		on update cascade
		on delete cascade
);

CREATE TABLE IF NOT EXISTS projects (
    project_id VARCHAR(4) PRIMARY KEY,
    project_name VARCHAR(100) NOT NULL,
    employee_id VARCHAR(4)
);

-- Data Manipulation languange (DML)
-- query DML insert, delete, update

INSERT INTO departments VALUES 
	('D001', 'Human Resources', 2),
	('D002', 'Engineering', 2),
	('D003', 'Marketing', 1);

INSERT INTO employees VALUES 
	('E001', 'D001', 'Budi Santoso', 'budi@company.com', 'LAKI-LAKI', 8500000),
	('E002', 'D001', 'Siti Aminah', 'siti@company.com', 'perempuan', 9000000),
	('E003', 'D002', 'Alex Wijaya', 'alex@company.com', 'LAKI-LAKI', 15000000),
	('E004', 'D002', 'Rani Permata', 'rani@company.com', 'PEREMPUAN', 14500000),
	('E005', 'D003', 'Rian Hidayat', 'rian@company.com', 'laki-laki', 7500000);

INSERT INTO projects VALUES 
	('P001', 'Migrasi Cloud AWS', 'E003'),  -- Dipimpin oleh Alex (Engineering)
	('P002', 'Rebranding Sosmed', 'E005'),   -- Dipimpin oleh Rian (Marketing)
	('P003', 'Audit Internal Q3', 'E001'),   -- Dipimpin oleh Budi (HR)
	('P004', 'Aplikasi Mobile AI', NULL);    -- Belum ada manajernya (Sengaja NULL)

-- Data Query Language (DQL)
-- DQL pada dasarnya hanya Select.

select * from departments;
select * from projects;
select * from employees;

select * 
	from employees
	where salary <= 9000000;

select lower(gender) as gender, sum(salary) as total_salary 
	from employees
	group by lower(gender)
	order by total_salary;

-- Data Control Languange (DCL)

-- Transaction Control Language (TCL)

-- Operators

-- Functions

-- Data Types



