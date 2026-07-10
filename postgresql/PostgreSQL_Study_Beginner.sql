select current_database();
show search_path;
set search_path to "example_company";
select table_name from information_schema.tables where table_schema = 'example_company';

--</> Data Definition Language (DDL)
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

--</> Data Manipulation languange (DML)
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

--</> Data Query Language (DQL)
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

--</> Data Control Languange (DCL)
select rolname from pg_roles; -- melihat semua roles
select * from pg_user; -- melihat daftar user db
select current_user; -- mengembalikan user sekarang

create user analyst with password 'password123'; -- membuat user
alter user analyst with password 'new_password'; -- mengubah password user
drop user analyst; -- menghapus user

-- memberi roles kepada user
-- contoh table
create table if not exists example(
	id BIGINT PRIMARY KEY,
    name VARCHAR(100)
);

drop table example;

-- cek hak pada user
SELECT * FROM information_schema.role_table_grants WHERE grantee = 'analyst';

grant SELECT on example to analyst;
grant INSERT on example to analyst;
grant UPDATE, DELETE on example to analyst;
grant ALL on example to analyst;

-- mencabut hak
revoke 
	DELETE,
	TRUNCATE,
	REFERENCES,
	TRIGGER
on example
from analyst;

--</> Transaction Control Language (TCL)
-- contoh tabel
create table if not exists accounts(
    account_id INT PRIMARY KEY,
    account_owner VARCHAR(50),
    balance NUMERIC
);

insert into accounts values 
	(1, 'Andi', 1000000),
	(2, 'Budi', 500000);

select * from accounts;

BEGIN; -- Memulai Transaction
update accounts
	set balance = balance - 100000
	where account_id = 1;

update accounts
	set balance = balance - 100000
	where account_id = 2;

ROLLBACK; -- membatalkan seluruh perubahan sejak begin
COMMIT; -- Menyimpan perubahan permanen

-- savepoint
BEGIN;
update accounts
	set account_owner = 'Budi Hartono'
	where account_id = 2;

SAVEPOINT sp1;

insert into accounts values (3, 'Melanin', 500000);

select * from accounts;
ROLLBACK to SAVEPOINT sp1;
ROLLBACK;
COMMIT;

--</> Operators


--</> Functions
create table if not exists stringtab(
	word1 TEXT not null,
	word2 text not null
);

insert into stringtab values 
	('fancy', 'jacket'),
	('stuard', 'hogward'),
	('doubt ', 'manakins'),
	('uncle', ' bob'),
	('adam', ' andalman ');

-- String Functions
select concat(word1, ' ', word2) from stringtab;
select concat_ws('_', word1, '->', word2) from stringtab;
select string_agg(word1, '_') from stringtab;
select
	word1,
	right(word1, 3) as tree_words_from_right,
	left(word1, 3) as tree_words_from_left
	from stringtab;
select word1, length(word1) from stringtab;
select word1, reverse(word1) from stringtab;
select word1, repeat(word1, 2) from stringtab;
select
	word1,
	SUBSTRING(word1 from 1 for length(word1) - 2)
	from stringtab;
select
	word1,
	upper(word1),
	lower(word1)
	from stringtab;
select
	replace(word1, 'a', 'i')
	from stringtab;
select
	word2,
	replace(word2, ' ', '_'),
	ltrim(word2),
	rtrim(word2),
	trim(word2)
	from stringtab;

-- Date Functions
CREATE TABLE example_time (
    waktu TIMESTAMP
);

INSERT INTO example_time VALUES
    ('2026-06-25 08:30:00'),
    ('2026-06-25 12:15:45'),
    ('2026-06-26 17:00:00'),
    ('2026-06-27 23:59:59');

select
	current_date,
	current_time,
	current_timestamp,
	now();
	
select
	extract(day from waktu),
	extract(month from waktu),
	extract(year from waktu),
	date(waktu) -- sama dengan current_date
	from example_time;
select
	waktu + interval '3 days'
	from example_time; -- menambah interval waktu
select
	current_timestamp as current,
	waktu,
	current_timestamp - waktu -- menghitung selisih waktu
	from example_time;

-- Numeric Function
create table if not exists example_math(
	number1 INT,
	number2 INT
);

insert into example_math values (3, 5), (13, 17), (21, 19), (10, 13), (31, 20);

SELECT
    akar_kuadrat,
    nilai_absolut,
    logaritma_natural,
    logaritma_10,
    ceiling(akar_kuadrat) AS pembulatan_keatas,
    floor(akar_kuadrat) AS pembulatan_kebawah,
    round(akar_kuadrat::decimal, 3) AS round_decimal,
    sign_value,
    perpangkatan,
    exponential,
    akar_4
FROM (
    SELECT
        sqrt(number1) AS akar_kuadrat,
        abs(number1) AS nilai_absolut,
        ln(number1) AS logaritma_natural,
        log(10, number1) AS logaritma_10,
        sign(number1) AS sign_value,
        power(number1, 2) AS perpangkatan,
        exp(number1) AS exponential,
    FROM example_math
) AS subquery;

-- Aggregation Function
select 
	avg(number1),
	min(number1),
	max(number1),
	stddev(number1),
	variance(number1),
	sum(number1),
	count(number1)
from example_math;

--</> Data Types



