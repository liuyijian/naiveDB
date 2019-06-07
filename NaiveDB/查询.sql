auth admin 123456;
create database database1;
use database database1;
drop table avengers;
drop table villain;

create table avengers
    (id         int not null, 
     name           string(32) not null, 
     power  int not null,
     primary key(ID)
    );

create table villain
    (id         int not null, 
     name           string(32) not null, 
     power  int not null,
     primary key(ID)
    );

INSERT INTO avengers VALUES (1, 'Captain', 10);
INSERT INTO avengers VALUES (2, 'Thor', 20);
INSERT INTO avengers VALUES (3, 'IronMan', 30);
INSERT INTO avengers VALUES (4, 'rocket', 40);
INSERT INTO avengers VALUES (5, 'Groot', 50);
INSERT INTO avengers VALUES (6, 'Groot1', 60);

INSERT INTO villain VALUES (1, 'Thanos', 100);
INSERT INTO villain VALUES (2, 'Red Skull', 80);
INSERT INTO villain VALUES (3, 'Hella', 60);
INSERT INTO villain VALUES (4, 'monster', 40);
INSERT INTO villain VALUES (6, 'Groot1', 60);

select avengers.name, villain.name, villain.power from avengers join villain where avengers.name = 'Thor' or avengers.power > 80 or villain.power > 80;
select * from avengers join villain on avengers.name = villain.name and avengers.power = villain.power or avengers.power > villain.power;
select * from avengers natural join avengers where avengers.id <= 4;
select avengers.name from avengers natural join villain;

