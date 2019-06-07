auth admin 123456;
create database query_test;
use database query_test;

drop table avengers;
drop table villain;

create table avengers
    (id         int not null, 
     name           string(32), 
     power  int not null,
     weight     float,
     primary key (ID, name)
    );

create table villain
    (id         int not null, 
     name           string(32), 
     power  int not null,
     primary key (ID, name)
    );

INSERT INTO avengers VALUES (10, 'Captain', 50, 78.1, 1.85);
INSERT INTO avengers VALUES (3, 'Thor', 90, 92.1, 1.89);
INSERT INTO avengers VALUES (7, 'IronMan', 85, 82.1, 1.76);
INSERT INTO avengers VALUES (4, 'rocket', 40, 42.1, 0.76);
INSERT INTO avengers VALUES (5, 'Groot', 10, 182.1, 2.76);
INSERT INTO villain VALUES (1, 'Thanos', 100);
INSERT INTO villain VALUES (2, 'Red Skull', 40);
INSERT INTO villain VALUES (3, 'Hella', 90);
INSERT INTO villain VALUES (4, 'monster', 10);

INSERT INTO avengers VALUES (10, null, 50, 78.1, 1.85);
INSERT INTO avengers VALUES (10, 'steve', null, 78.1, 1.85);
INSERT INTO avengers VALUES (10, 'steve', 100, 78.1, 1.85);

show table avengers;
show table villain;

select avengers.name, villain.name, villain.power from avengers join villain on avengers.power = villain.power where villain.power > 40;
select * from avengers join villain on avengers.power = villain.power where villain.power > 40;
select * from avengers;
select id, name from avengers where id = 4;
select * from avengers natural join avengers where avengers.id <= 4;
