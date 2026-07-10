select current_database();
show search_path;
set search_path to public;
select table_name from information_schema.tables where table_schema = 'public';

-- XX QUERY DASAR XX

--</> create table

create table if not exists customers(
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

create table if not exists products(
	productId varchar(5),
	productName varchar(50) not null,
	unitPrice decimal not null default 0,
	unitStock integer not null default 0,
	unitOrder integer not null default 0,
	CONSTRAINT PK_products PRIMARY KEY (productId)
);

create table if not exists orders(
	orderId varchar(5),
	customerId varchar(5) not null,
	orderDate timestamp not null default current_timestamp,
	CONSTRAINT PK_orders PRIMARY KEY (orderId),
	CONSTRAINT FK_orders FOREIGN KEY (customerId) references customers(customerId)
		on update cascade
		on delete cascade
);

create table if not exists order_details(
	orderId varchar(5) not null,
	productId varchar(5) not null,
	unitPrice decimal not null default 0,
	quantity integer not null default 0
);

--</> alter table

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

--</> drop table

drop table customers;

--</> insert table

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

--</> update

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

--</> delete

delete from products
	where productid = 'A0002';

delete from products; -- beware with this.. 

--</> Select

select * from products;
select * from products where productname like 'Mo%';
select * from products where productname like '%Pad';

--</> Order by
select * from products order by unitprice asc;
select * from products order by unitprice desc;

--</> Conditional Expression
select
	case
		when unitstock = 0
		then 'Run Out Stock'
		else 'Stock Available'
	end
from products;

--</> Sub Query

select 
	*,
	(
		case
			when unitstock = 0
			then 'Run Out Stock'
			else 'Stock Available'
		end
	) as status
	from products;

--</> LOOP
DO $$
DECLARE
	emp_record RECORD;
	total DECIMAL(10, 2) := 0;
	counter INT := 0;
BEGIN
	FOR emp_record IN
		select employee_name, salary from employees order by salary desc
	LOOP
		counter := counter + 1;
		total := total + emp_record.salary;
		RAISE NOTICE '%: % (RP %)', counter, emp_record.employee_name, emp_record.salary;
	END LOOP;

	RAISE NOTICE 'TOTAL gaji: RP %', total;
	RAISE NOTICE 'RATA-rata gaji: RP %', total / counter;
END;
$$;

select * from employees;


--</> Procedure
CREATE TABLE if not exists accounts (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    balance DEC(15,2) NOT NULL
);

INSERT INTO accounts VALUES 
	(1, 'Bob', 10000),
	(2, 'Alice', 10000);

-- contoh 1
create or replace procedure transfer(
	sender INT,
	receiver INT,
	amount DEC
)
LANGUAGE plpgsql
AS $$
BEGIN
	Update accounts set balance = balance - amount where id = sender;
	Update accounts set balance = balance + amount where id = receiver;
	COMMIT;
END;
$$;

select * from accounts;
CALL transfer(1, 2, 1000);

-- contoh 2:
create or replace procedure faktorial(
	IN p_number INT,
	INOUT p_result INT DEFAULT 1
)
LANGUAGE plpgsql
AS $$
DECLARE
	v_X INT := 1;
	v_faktorial INT := 1;
BEGIN
	IF p_number < 0 THEN
		RAISE EXCEPTION 'faktorial tidak terdefinisi untuk angka negarif.';
	END IF;

	WHILE v_X <= p_number LOOP
		v_faktorial := v_faktorial * v_x;
		v_x := v_x 	+ 1;
	END LOOP;

	p_result := v_faktorial;
END;
$$;

CALL faktorial(5);


--</> FUNCTION
CREATE OR REPLACE FUNCTION factorial(
	num INT
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
	x INT := 1;
	res BIGINT := 1;
BEGIN
	WHILE x <= num LOOP
		res := res * x;
		x := x + 1;
	END LOOP;

	RETURN res;
END;
$$;

select angka, factorial(angka) from (VALUES(3), (5), (7)) as t(angka);

select * from information_schema.routines where routine_type = 'PROCEDURE'; -- cek daftar procedure
select * from information_schema.routines where routine_type = 'FUNCTION'; -- cek daftar fungsi (banyak)

--</> CTE


--</> TRIGGER
create table if not exists salary_history(
    employee_id INT,
    old_salary NUMERIC,
    new_salary NUMERIC,
    changed_at TIMESTAMP
);

CREATE OR REPLACE FUNCTION audit_salary()
returns TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
	INSERT INTO salary_history values (
		NEW.employee_id,
		OLD.salary,
		NEW.salary,
		now()
	);

	return NEW;
END;
$$;

CREATE OR REPLACE TRIGGER trg_salary
AFTER UPDATE -- ADA VERSI BEFORE, TRIGGER terjadi bila ada event seperti  insert, update, delete
ON employees
FOR EACH ROW
EXECUTE FUNCTION audit_salary();

-- example used
select * from employees;
select * from salary_history;
update employees
	set salary = 8200000
	where employee_id = 1;