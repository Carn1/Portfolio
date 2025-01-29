CREATE TABLE if not exists ashkinadzi.clients (
client_id integer,
client_name varchar(40),
client_email varchar(40),
client_phone varchar(40),
client_address varchar(80));


alter table ashkinadzi.clients add primary key (client_id);

alter table ashkinadzi.clients add column updated_at timestamp default NOW(); 

alter table ashkinadzi.clients rename to clients_buff;
