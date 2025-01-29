CREATE TABLE if not exists ashkinadzi.currencies_buff (
ID varchar(10),
NumCode varchar(10),
CharCode varchar(10),
Nominal integer,
Name varchar(80),
Value numeric,
Date date,
updated_at timestamp default NOW());







