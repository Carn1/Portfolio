CREATE TABLE if not exists ashkinadzi.security_transactions (
client_id integer,
security_id integer,
transaction_date timestamp,
transaction_type varchar(5),
quantity integer,
currency varchar(3),
price numeric);

alter table ashkinadzi.security_transactions add column security_transaction_id serial;

alter table ashkinadzi.security_transactions add primary key (security_transaction_id);

alter table ashkinadzi.security_transactions add foreign key (client_id) references ashkinadzi.clients (client_id);

alter table ashkinadzi.security_transactions add foreign key (security_id) references ashkinadzi.securities (security_id);

alter table ashkinadzi.security_transactions add column updated_at timestamp default NOW(); 

alter table ashkinadzi.security_transactions rename to security_transactions_buff;