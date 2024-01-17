CREATE SCHEMA "test.gt_mysql".gt_db1;

USE "test.gt_mysql".gt_db1;

-- Unsupported Type: BOOLEAN
CREATE TABLE tb01 (
    f1 VARCHAR(200),
    f2 CHAR(20),
    f3 VARBINARY,
    f4 DECIMAL(10, 3),
    f5 REAL,
    f6 DOUBLE,
    f8 TINYINT,
    f9 SMALLINT,
    f10 INT,
    f11 INTEGER,
    f12 BIGINT,
    f13 DATE,
    f14 TIME,
    f16 TIMESTAMP
);

SHOW CREATE TABLE tb01;

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb01 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

select * from tb01;

CREATE TABLE tb02 (
    f1 VARCHAR(200) not null ,
    f2 CHAR(20) not null ,
    f3 VARBINARY not null ,
    f4 DECIMAL(10, 3) not null ,
    f5 REAL not null ,
    f6 DOUBLE not null ,
    f8 TINYINT not null ,
    f9 SMALLINT not null ,
    f10 INT not null ,
    f11 INTEGER not null ,
    f12 BIGINT not null ,
    f13 DATE not null ,
    f14 TIME not null ,
    f16 TIMESTAMP not null
);

show create table tb02;

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES ('Sample text 1', 'Text1', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES ('Sample text 1', NULL, x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 100000, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES ('Sample text 1', 'same3', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, NULl, DATE '2024-01-01', TIME '08:00:00', TIMESTAMP '2024-01-01 08:00:00');

INSERT INTO tb02 (f1, f2, f3, f4, f5, f6, f8, f9, f10, f11, f12, f13, f14, f16)
VALUES ('Sample text 1', 'same9', x'65', 123.456, 7.89, 12.34, 1, 100, 1000, 1000, 1992382342, DATE '2024-01-01', NULL, TIMESTAMP '2024-01-01 08:00:00');

drop table tb01;

drop table tb02;

drop schema "test.gt_mysql".gt_db1 cascade;
