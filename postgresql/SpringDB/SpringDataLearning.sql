select current_database();
show search_path;
set search_path to spring_learning;

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