CREATE TABLE if not exists ashkinadzi.securities (
security_id integer,
security_name varchar(40),
security_type varchar(10),
market varchar(10));

alter table ashkinadzi.securities add primary key (security_id);

alter table ashkinadzi.securities add column updated_at timestamp default NOW(); 

alter table ashkinadzi.securities rename to securities_buff;