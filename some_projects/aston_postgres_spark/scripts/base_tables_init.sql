CREATE TABLE if not exists ashkinadzi.clients_base (
client_id integer primary key,
client_name varchar(40),
client_email varchar(40),
client_phone varchar(40),
client_address varchar(80),
updated_at timestamp,
status varchar(5) default 'a');


CREATE TABLE if not exists ashkinadzi.securities_base (
security_id integer primary key,
security_name varchar(40),
security_type varchar(10),
market varchar(10),
updated_at timestamp,
status varchar(5) default 'a');


CREATE TABLE if not exists ashkinadzi.security_transactions_base (
client_id integer,
security_id integer,
transaction_date timestamp,
transaction_type varchar(5),
quantity integer,
currency varchar(3),
price numeric,
security_transaction_id serial primary key,
updated_at timestamp,
status varchar(5) default 'a',

foreign key (client_id) references ashkinadzi.clients_base (client_id),
foreign key (security_id) references ashkinadzi.securities_base (security_id)
);


CREATE TABLE if not exists ashkinadzi.transactions_base (
client_id integer,
transaction_id integer,
transaction_date timestamp,
transaction_type varchar(20),
account_number varchar(22),
currency varchar(3),
amount numeric,
bank_transaction_id UUID primary key default gen_random_uuid(),
updated_at timestamp,
status varchar(5) default 'a',

foreign key (client_id) references ashkinadzi.clients_base (client_id)
);

CREATE TABLE if not exists ashkinadzi.currencies_base (
ID varchar(10),
NumCode varchar(10),
CharCode varchar(10),
Nominal integer,
Name varchar(80),
Value numeric,
Date date,
updated_at timestamp,
status varchar(5) default 'a');

