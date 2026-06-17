select current_database();
show search_path;

-- dataset dari Test1_level_1

-- soal 1: Tampilkan seluruh karyawan yang:
-- 1. bekerja di departemen IT; dan
-- 2. salary lebih dari 8.000.000.

select * from employees where department = 'IT' and salary > 8000000;

-- soal 2: Tampilkan seluruh karyawan yang:
-- 1. berumur di bawah 30 tahun;
-- 2. atau bekerja di departemen HR.

select * from employees where age < 30 or department = 'HR';

-- soal 3: Tampilkan seluruh karyawan yang memiliki salary antara 7.000.000 sampai 9.000.000
select * from employees where salary between 7000000 and 9000000;

-- soal 4: Tampilkan seluruh karyawan yang bekerja pada salah satu departemen berikut:
-- IT, Finance, Marketing

select * from employees where department in ('IT', 'Finance', 'Marketing');

-- soal 5: Tampilkan seluruh karyawan yang namanya diawali huruf A
select * from employees where left(employee_name, 1) = 'A';

-- soal 6: Tampilkan seluruh karyawan yang namanya mengandung huruf: i
select * from employees where employee_name like '%i%';

-- soal 7: Hitung jumlah karyawan yang bekerja di departemen IT.
select count(*) as total_it_employee from employees where department = 'IT';

-- soal 8: Hitung total salary seluruh karyawan.
select sum(salary) as total_salary from employees; 

-- soal 9: Cari salary tertinggi dari seluruh karyawan.
select max(salary) as max_salary from employees;

-- soal 10: Cari salary terendah dari seluruh karyawan.
select min(salary) as min_salary from employees;

-- soal 11: Hitung rata-rata salary untuk departemen IT saja.
select avg(salary) as avg_it_salary from employees where department = 'IT';

-- soal 12: Tampilkan jumlah karyawan per departemen, urutkan dari jumlah terbanyak ke paling sedikit.
select department, count(*) as total_employee 
	from employees 
	group by department
	order by total_employee desc;

-- soal 13: Tampilkan departemen yang memiliki lebih dari 2 karyawan.
select department, count(*) as total_employee from employees 
	group by department
	having total_employee > 2;

-- soal 14: Tampilkan:
-- 1. nama karyawan;
-- 2. salary;
-- 3. salary tahunan.

select employee_name, salary, salary * 12 as annual_salary from employees;

-- soal 15: Tampilkan 5 karyawan termuda.
-- Urutkan dari yang paling muda terlebih dahulu.

select * from employees
	order by age ASC
	limit 5;