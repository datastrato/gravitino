CREATE SCHEMA "test.gt_postgresql".varchar_db1;

USE "test.gt_postgresql".varchar_db1;

CREATE TABLE tb01 (id int, name char(20));

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb01;

CREATE TABLE tb02 (id int, name char(65536));

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb02;

CREATE TABLE tb03 (id int, name char(65537));

CREATE TABLE tb04 (id int, name varchar(250));

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb04;

CREATE TABLE tb05 (id int, name varchar(10485760));

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb05;

CREATE TABLE tb06 (id int, name varchar(10485761));

CREATE TABLE tb06 (id int, name char);

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb06;

CREATE TABLE tb07 (id int, name varchar);

SHOW CREATE TABLE "test.gt_postgresql".varchar_db1.tb07;

drop table "test.gt_postgresql".varchar_db1.tb01;

drop table "test.gt_postgresql".varchar_db1.tb02;

drop table "test.gt_postgresql".varchar_db1.tb04;

drop table "test.gt_postgresql".varchar_db1.tb05;

drop schema "test.gt_postgresql".varchar_db1;

