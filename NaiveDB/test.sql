auth admin 123456;
use database database1;
create table avengers
	(id			int not null, 
	 name			string(32) not null, 
	 power	int not null,
	 weight     float,
	 primary key (ID)
	);

drop table avengers;

create table avengers
	(id			int not null, 
	 name			string(32) not null, 
	 power	int not null,
	 weight     float,
	 height     double,
	 primary key (ID)
	);

INSERT INTO avengers VALUES (10, 'Captain', 50, 78.1, 1.85);
INSERT INTO avengers VALUES (3, 'Thor', 90, 92.1, 1.89);
INSERT INTO avengers VALUES (7, 'IronMan', 85, 82.1, 1.76);
INSERT INTO avengers VALUES (4, 'rocket', 40, 42.1, 0.76);
INSERT INTO avengers VALUES (5, 'Groot', 10, 182.1, 2.76);
INSERT INTO avengers VALUES (6, 'Groot1', 10, 182.1, 2.76);

DELETE FROM avengers WHERE name = 'Groot';

UPDATE avengers SET power = 100 WHERE name = 'Captain';

create table villain
	(id			int not null, 
	 name			string(32) not null, 
	 power	int not null,
	 primary key (ID)
	);

INSERT INTO villain VALUES (1, 'Thanos', 100);
INSERT INTO villain VALUES (2, 'Red Skull', 40);
INSERT INTO villain VALUES (3, 'Hella', 90);
INSERT INTO villain VALUES (4, 'monster', 10);
INSERT INTO villain VALUES (6, 'Groot1', 10);

show table avengers;

select name from avengers where id < name;
select avengers.name, villain.name, villain.power from avengers join villain on avengers.power = villain.power where villain.power > 40;
select * from avengers join villain on avengers.power = villain.power where villain.power > 40;

select * from avengers;

select id, name from avengers where id = 4;
select * from avengers natural join avengers where avengers.id <= 4;

select avengers.name from avengers natural join villain;
