create schema lab;
set search_path to lab;

create table buku (
	id_buku varchar(9) not null,
	nama_buku varchar(100) not null,
	genre varchar(100) not null,
	penulis varchar(100) not null,
	halaman int not null,
	constraint pk_buku primary key(id_buku)
);

create table members(
	member_id varchar(9) not null,
	member_name varchar(100) not null,
	sex varchar(10) check(sex in ('male', 'Male', 'female', 'Female')) not null, 
	age int not null,
	start_membership Date not null,
	lots_of_borrowing int default 0,
	constraint PK_members primary key(member_id)
);

insert into buku values
	('A01', 'Old Jenkins cant War?', 'History', 'The Hash Slinging Slasher', 300),
	('A02', 'The Man Who don''t know Sins', 'Psychology', 'Teddy Armstrong', 345),
	('A03', 'Cosmos: What is it?', 'Sains', 'Round Robin', 147),
	('A04', 'World Must Crazy?', 'History', 'The Hash Slinging Slasher', 399),
	('A05', '100th of Gravity Math', 'Sains', 'Joe Mama', 370),
	('A06', 'Hash The Golden Cow', 'Psychology', 'Yon Askhaniggham', 190);

INSERT INTO members (member_id, member_name, sex, age, start_membership, lots_of_borrowing) VALUES
	('M001', 'Andi Wijaya', 'Male', 25, NOW(), 3),
	('M002', 'Siti Aminah', 'Female', 30, NOW(), 5),
	('M003', 'Budi Santoso', 'Male', 28, NOW(), 2),
	('M004', 'Rina Marlina', 'Female', 27, NOW(), 4),
	('M005', 'Ahmad Fauzi', 'Male', 35, NOW(), 7),
	('M006', 'Dewi Sartika', 'Female', 22, NOW(), 1),
	('M007', 'Tono Supriyadi', 'Male', 29, NOW(), 6),
	('M008', 'Lina Kusuma', 'Female', 24, NOW(), 0),
	('M009', 'Rizky Hidayat', 'Male', 31, NOW(), 5),
	('M010', 'Sari Wulandari', 'Female', 26, NOW(), 3);

select * from buku;
alter table buku rename to books;
alter table books rename column id_buku to book_id;
alter table books rename nama_buku to book_name;
alter table books rename penulis to author;
alter table books rename halaman to pages;
alter table books rename constraint pk_buku to pk_books;

select * from books;
select * from members;
