select current_database();
show search_path;

create table if not exists employees (
    employee_id SERIAL PRIMARY KEY,
    employee_name VARCHAR(100) NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary NUMERIC(12,2) NOT NULL,
    age INT NOT NULL,
    join_date DATE NOT NULL
);

INSERT INTO employees (
    employee_name,
    department,
    salary,
    age,
    join_date
)
VALUES
('Andi', 'IT', 8000000, 25, '2022-01-10'),
('Budi', 'Finance', 7500000, 30, '2021-05-15'),
('Citra', 'HR', 6500000, 28, '2023-03-01'),
('Doni', 'IT', 9500000, 35, '2020-08-20'),
('Eka', 'Marketing', 7000000, 27, '2022-11-11'),
('Fajar', 'Finance', 8500000, 32, '2019-09-05'),
('Gina', 'IT', 10000000, 29, '2021-12-12'),
('Hani', 'HR', 6200000, 24, '2024-01-15'),
('Indra', 'Marketing', 7200000, 31, '2020-06-30'),
('Joko', 'IT', 8800000, 26, '2023-07-21');

-- soal 1: Tampilkan seluruh data dari tabel employees.
select * from employees;

-- soal 2: Tampilkan hanya nama dan departemen seluruh karyawan.
select employee_name, department from employees;

-- soal 3: Tampilkan seluruh karyawan yang bekerja di departemen IT.
select * from employees where department = 'IT';

-- soal 4: Tampilkan seluruh karyawan yang memiliki salary lebih dari 8.000.000.
select * from employees where salary > 8000000;

-- soal 5: Tampilkan seluruh karyawan yang berumur kurang dari 30 tahun.
select * from employees where age < 30;

-- soal 6: Urutkan seluruh karyawan berdasarkan salary tertinggi ke terendah.
select * from employees order by salary desc;

-- soal 7: Tampilkan 3 karyawan dengan salary tertinggi.
select * from employees order by salary desc limit 3;

-- soal 8: Hitung jumlah seluruh karyawan.
select count(*) as total_employee from employees;

-- soal 9: Hitung rata-rata salary seluruh karyawan.
select avg(salary) as avg_salary from employees;

-- soal 10: Hitung jumlah karyawan pada setiap departemen.
select department, count(*) as total_employee from employees group by department;