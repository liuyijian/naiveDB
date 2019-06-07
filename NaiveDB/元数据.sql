auth admin 123456;
create database syr;
create database jt;
create database ayz;
create database qjl;
show databases;

use database syr;
create table avengers
	(id			int not null, 
	 name			string(32) not null, 
	 power	int not null,
	 weight     float,
	 primary key (ID)
	);

INSERT INTO avengers VALUES (10, 'Captain', 50, 78.1, 1.85);
INSERT INTO avengers VALUES (3, 'Thor', 90, 92.1, 1.89);
INSERT INTO avengers VALUES (7, 'IronMan', 85, 82.1, 1.76);
INSERT INTO avengers VALUES (4, 'rocket', 40, 42.1, 0.76);
INSERT INTO avengers VALUES (5, 'Groot', 10, 182.1, 2.76);


use database ayz;
create table avengers
	(id			int not null, 
	 name			string(32) not null, 
	 power	int not null,
	 weight     float,
	 height     double,
	 primary key (ID)
	);
create table villain
    (id         int not null, 
     name           string(32), 
     power  int not null,
     primary key (ID, name)
    );

show database syr;
show database ayz;
show database jt;
drop database avengers;
drop database ayz;

show database ayz;
show databases;
