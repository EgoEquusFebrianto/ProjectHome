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