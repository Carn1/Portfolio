CREATE TABLE if not exists ashkinadzi.transactions (
client_id integer,
transaction_id integer,
transaction_date timestamp,
transaction_type varchar(20),
account_number varchar(22),
currency varchar(3),
amount numeric);

alter table ashkinadzi.transactions add column bank_transaction_id UUID;

update ashkinadzi.transactions set bank_transaction_id = gen_random_uuid();

alter table ashkinadzi.transactions add primary key (bank_transaction_id);

alter table ashkinadzi.transactions add foreign key (client_id) references ashkinadzi.clients (client_id);

alter table ashkinadzi.transactions alter column bank_transaction_id set default gen_random_uuid();

alter table ashkinadzi.transactions add column updated_at timestamp default NOW(); 

alter table ashkinadzi.transactions rename to transactions_buff;

