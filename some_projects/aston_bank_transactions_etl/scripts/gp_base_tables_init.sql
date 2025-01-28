-- Создание базовых таблиц в Greenplum, в которые попадают данные из буферных таблиц
--------------------------------------------------------------------------------------------
-- gp_clients_base
drop table if exists ashkinadzi.gp_clients_base;
create table if not exists ashkinadzi.gp_clients_base 
(
	client_id BIGINT primary key,
    client_name TEXT,
    client_email TEXT,
    client_phone TEXT,
    client_address TEXT,
    updated_at TIMESTAMP default NOW(),
    status varchar(5) default 'a'
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id);


-- gp_securities_base
drop table if exists ashkinadzi.gp_securities_base;
create table if not exists ashkinadzi.gp_securities_base
(
	security_id BIGINT primary key,
	security_name TEXT,
	security_type TEXT,
	market TEXT,
	updated_at TIMESTAMP default NOW(),
	status varchar(5) default 'a'
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (security_id);


-- gp_currencies_base
drop table if exists ashkinadzi.gp_currencies_base;
create table if not exists ashkinadzi.gp_currencies_base 
(
	ID TEXT,
	NumCode TEXT,
	CharCode TEXT,
	Nominal integer,
	Name TEXT,
	Value NUMERIC,
	Date DATE,
	updated_at TIMESTAMP default NOW(),
	status varchar(5) default 'a'
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (CharCode);


-- gp_security_transactions_base
drop table if exists ashkinadzi.gp_security_transactions_base;
create table if not exists ashkinadzi.gp_security_transactions_base 
(
	client_id BIGINT references ashkinadzi.gp_clients_base (client_id),
	security_id BIGINT references ashkinadzi.gp_securities_base (security_id),
	transaction_date TIMESTAMP,
	transaction_type TEXT,
	quantity INTEGER,
	currency TEXT,
	price NUMERIC,
	security_transaction_id bigserial,
	updated_at TIMESTAMP default NOW(),
	status varchar(5) default 'a'
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id, security_id, transaction_date);


-- gp_bank_transactions_base

drop table if exists ashkinadzi.gp_bank_transactions_base;
create table if not exists ashkinadzi.gp_bank_transactions_base 
(
	client_id BIGINT references ashkinadzi.gp_clients_base (client_id),
	transaction_id BIGINT,
	transaction_date TIMESTAMP,
	transaction_type TEXT,
	account_number TEXT,
	currency TEXT,
	amount NUMERIC,
	--bank_transaction_id UUID default uuid_cb_generate() ,
	updated_at TIMESTAMP default NOW(),
	status varchar(5) default 'a'
)
WITH (appendonly=TRUE, 
	  orientation=COLUMN, 
	  compresslevel=1, 
	  compresstype=zstd)
DISTRIBUTED BY (client_id, transaction_id, transaction_date);

grant all on gp_clients_base, gp_securities_base, gp_currencies_base, gp_security_transactions_base, gp_bank_transactions_base to wave15_user_a3;
