select current_database();
show search_path;
select table_name from information_schema.tables where table_schema = 'public';

create table mahasiswa(
	mhs_id varchar(9) not null,
	mhs_name varchar(255) not null,
	semester SMALLINT not null,
	sks_lulus integer not null,
	constraint pk_mahasiswa primary key(mhs_id)
);

CREATE TABLE matkul
(
	kd_mk VARCHAR(6),
    nm_mk VARCHAR(50) NOT NULL,
    sks INTEGER NOT NULL,
    semester INTEGER NOT NULL,
    ket TEXT,
    constraint PK_Matkul PRIMARY KEY (kd_mk)
);

CREATE TABLE mhs
(
	nim CHAR(9),
    nama_mhs VARCHAR(50) NOT NULL,
    jurusan CHAR(2) NOT NULL,
    tgl_lahir DATE NOT NULL,
    constraint PK_Mhs PRIMARY KEY (nim)
);

CREATE TABLE nilai
(
	ta CHAR(8),
    nim CHAR(9),
	kd_mk CHAR(6),
    nilai_angka INTEGER,
    nilai_huruf CHAR(2),
    constraint PK_Nilai PRIMARY KEY (ta, nim, kd_mk),
    constraint FK_Nilai_Mhs FOREIGN KEY (nim) REFERENCES mhs (nim)
		ON UPDATE CASCADE ON DELETE CASCADE,
    constraint FK_Nilai_Matkul FOREIGN KEY (kd_mk) REFERENCES matkul (kd_mk)
		ON UPDATE CASCADE ON DELETE CASCADE
);

select * from mahasiswa;
select column_name, data_type
	from information_Schema.columns
	where table_name = 'mahasiswa' and column_name = 'mhs_id';
drop table mahasiswa;
insert into mahasiswa values 
	('B47', 'Si Ucok', 5, 89);
update mahasiswa set sks_lulus = 119 where mhs_id = 'B47';	
delete from mahasiswa where mhs_id = 'B47';

alter table mahasiswa
	alter column mhs_id type integer using mhs_id::integer, -- cast tipe data lama (varchar) ke integer
	alter column mhs_id set not null;
alter table mahasiswa
	add column gender varchar(1) check (gender in ('L', 'P')) not null;
alter table mahasiswa
	drop column gender;
alter table mahasiswa
	rename column mhs_name to nama_mhs;

-- Alter Method

-- 1. Menambah kolom baru
-- fomula: ADD [Column] col_name col_definition

Alter table employees 
	add column age integer,
	add column address text default '-';


-- 2. Menambah constraint naming

-- 2.1 Constraint pengecekan email domain
Alter table employees
add constraint check_email 
check (
	email like '%@staf.ai.ac.id' or
	email like '%@staf.dataengineer.ac.id'
);

-- atau dengan teknik lain dari diatas

Alter table employees
add constraint check_email
check (
	SUBSTRING(email from position('@' in email)) in (
		'@staf.ai.ac.id',
		'@staf.dataengineer.ac.id'
	)
);

-- 2.2. Constraint Foreign Key (...) References ...
Alter table employees
add constraint fk_employee
foreign key(department_id) references departments(department_id) 
	on update cascade on delete cascade;

-- 2.3 Constraint Check Expression
Alter table employees
add constraint check_salary
check (salary > 50000);


-- 3. Drop {Constraint | Check }
Alter table employees
drop constraint check_salary;


-- 4. Alter Constraint {Validate and Not Valid}
Alter table employees
add constraint check_email
check(
	substring(email from position('@' in email)) in (
		'@staf.ai.ac.id',
		'@staf.dataengineer.ac.id'		
	)
)
not valid; 

-- setelah data sudah valid, ubah validate constraint secara langsung dengan,

Alter table employees
validate constraint check_email;


-- 5. Constraint Behavior Deferability (deferrable/not deferrable)

-- 5.1. Not Deferrable (Langsung proses), Konsep dari "proses secara langsung setiap data" 

Alter table accounts
add constraint positive_balance
check (balance >= 0) NOT DEFERRED; -- Secara default NOT DEFERRED diaplikasikan, jadi tidak perlu di ketik

-- 5.2 Deferrable Initially Immediate, Konsep dari "Di proses langsung, namun dapat ditunda" 

Alter table employees drop constraint if exists fk_department;
Alter table employees
add constraint fk_department
Foreign key(department_id) references departments(department_id)
	on update cascade
	on delete cascade
DEFERRABLE INITIALLY IMMEDIATE;

-- 5.3 Deferrable Initially Deferred, Konsep dari "Dicek saat commit"

Alter table employees drop constraint if exists fk_department;
Alter table employees
add constraint fk_department
Foreign key(department_id) references departments(department_id)
	on update cascade
	on delete cascade
DEFERRABLE INITIALLY DEFERRED;

-- praktiknya, gunakan TCL (Transaction Control Language)

Update departments set department_id = 'DEE2' where department_id = 'DEE1';

BEGIN

Set constraint positive_balance DEFERRED;

Update accounts set balance = balance - 600000 where name_account = 'Bob';
Update accounts set balance = balance - 600000 where name_account = 'Alice';

COMMIT;


insert into departments values
	('DEE1', 'Data Management'),
	('DEE2', 'Artificial Intelligence');

insert into employees values 
	('AB01', 'DEE1', 'Round Robin', 'roundrobin123@staf.dataengineer.ac.id', 'laki-laki', 85000),
	('AB02', 'DEE2', 'Robin Hood', 'robinhood123@staf.ai.ac.id', 'laki-laki', 86000),
	('AB03', 'DEE1', 'Hidal Manaque', 'hidalmanaque123@staf.dataengineer.ac.id', 'laki-laki', 87000);
	
select * from employees;
select * from departments;

drop table departments;
drop table  employees;
