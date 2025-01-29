CREATE TABLE if not exists user3.clients (
client_id serial primary key,
client_name varchar(40) not null,
client_email varchar(40) unique not null,
client_phone varchar(40) not null,
client_address varchar(80) not null

);


CREATE TABLE if not exists user3.securities (
security_id integer primary key,
security_name varchar(40) not null,
security_type varchar(10) not null,
market varchar(10) not null

);


CREATE TABLE if not exists user3.transactions (
client_id integer not null,
transaction_id integer not null,
transaction_date timestamp not null,
transaction_type varchar(20) not null,
account_number varchar(22) not null,
currency varchar(3) not null,
amount numeric not null,
foreign key (client_id) references clients(client_id)

);


CREATE TABLE if not exists user3.security_transactions (
client_id integer not null,
security_id integer not null,
transaction_date timestamp not null,
transaction_type varchar(5) not null,
quantity integer not null,
currency varchar(3) not null,
price numeric not null


);