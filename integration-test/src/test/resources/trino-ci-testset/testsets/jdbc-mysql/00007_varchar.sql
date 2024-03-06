/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

CREATE SCHEMA "test.gt_mysql".varchar_db1;

USE "test.gt_mysql".varchar_db1;

CREATE TABLE tb01 (id int, name char(20));

SHOW CREATE TABLE "test.gt_mysql".varchar_db1.tb01;

CREATE TABLE tb02 (id int, name char(255));

SHOW CREATE TABLE "test.gt_mysql".varchar_db1.tb02;

CREATE TABLE tb03 (id int, name char(256));

CREATE TABLE tb04 (id int, name varchar(250));

SHOW CREATE TABLE "test.gt_mysql".varchar_db1.tb04;

CREATE TABLE tb05 (id int, name varchar(256));

SHOW CREATE TABLE "test.gt_mysql".varchar_db1.tb05;

drop table "test.gt_mysql".varchar_db1.tb01;

drop table "test.gt_mysql".varchar_db1.tb02;

drop table "test.gt_mysql".varchar_db1.tb04;

drop table "test.gt_mysql".varchar_db1.tb05;

drop schema "test.gt_mysql".varchar_db1;

