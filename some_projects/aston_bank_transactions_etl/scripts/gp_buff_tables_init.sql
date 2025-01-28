-- Создание буфферных таблиц в Greenplum, в которые попадают данные из внешних таблиц
--------------------------------------------------------------------------------------------
-- gp_clients_buff
drop table if exists ashkinadzi.gp_clients_buff;
create table if not exists ashkinadzi.gp_clients_buff 
(
	client_id BIGINT primary key,
    client_name TEXT,
    client_email TEXT,
    client_phone TEXT,
    client_address TEXT,
    updated_at TIMESTAMP default NOW()    
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id);


-- gp_securities_buff
drop table if exists ashkinadzi.gp_securities_buff;
create table if not exists ashkinadzi.gp_securities_buff 
(
	security_id BIGINT primary key,
	security_name TEXT,
	security_type TEXT,
	market TEXT,
	updated_at TIMESTAMP default NOW()   
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (security_id);


-- gp_currencies_buff
drop table if exists ashkinadzi.gp_currencies_buff;
create table if not exists ashkinadzi.gp_currencies_buff 
(
	ID TEXT,
	NumCode TEXT,
	CharCode TEXT,
	Nominal integer,
	Name TEXT,
	Value NUMERIC,
	Date DATE,
	updated_at TIMESTAMP default NOW()   
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (CharCode);


-- gp_security_transactions_buff
drop table if exists ashkinadzi.gp_security_transactions_buff;
create table if not exists ashkinadzi.gp_security_transactions_buff 
(
	client_id BIGINT references ashkinadzi.gp_clients_buff (client_id),
	security_id BIGINT references ashkinadzi.gp_securities_buff (security_id),
	transaction_date TIMESTAMP,
	transaction_type TEXT,
	quantity INTEGER,
	currency TEXT,
	price NUMERIC,
	security_transaction_id bigserial,
	updated_at TIMESTAMP default NOW()
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id, security_id, transaction_date);


-- gp_bank_transactions_buff

drop table if exists ashkinadzi.gp_bank_transactions_buff;
create table if not exists ashkinadzi.gp_bank_transactions_buff 
(
	client_id BIGINT references ashkinadzi.gp_clients_buff (client_id),
	transaction_id BIGINT,
	transaction_date TIMESTAMP,
	transaction_type TEXT,
	account_number TEXT,
	currency TEXT,
	amount NUMERIC,
	--bank_transaction_id UUID default uuid_cb_generate() ,
	updated_at TIMESTAMP default NOW()
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id, transaction_id, transaction_date);

grant all on gp_clients_buff, gp_securities_buff, gp_currencies_buff, gp_security_transactions_buff, gp_bank_transactions_buff to wave15_user_a3;

 
