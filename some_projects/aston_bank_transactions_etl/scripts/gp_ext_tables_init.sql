select current_schema;


-- Создание внешних таблиц в Greenplum, которые будет содержать данные из csv файлов в HDFS
--------------------------------------------------------------------------------------------
-- clients2.csv
DROP EXTERNAL TABLE ashkinadzi.ext_clients;
CREATE EXTERNAL TABLE  ashkinadzi.ext_clients
(
    client_id BIGINT,
    client_name text,
    client_email text,
    client_phone text,
    client_address text    
)
LOCATION ('pxf://user/s.ashkinadzi/clients2.csv?PROFILE=hdfs:text&HDFS_HOST=172.17.0.23&HDFS_PORT=8020')
FORMAT 'CSV' (header delimiter as ';');

-- securities2.csv
DROP EXTERNAL TABLE ashkinadzi.ext_securities;
CREATE EXTERNAL TABLE  ashkinadzi.ext_securities
(
    security_id BIGINT,
    security_name text,
    security_type text,
    market text
)
LOCATION ('pxf://user/s.ashkinadzi/securities2.csv?PROFILE=hdfs:text&HDFS_HOST=172.17.0.23&HDFS_PORT=8020')
FORMAT 'CSV' (header delimiter as ';');

-- currencies.csv
DROP EXTERNAL TABLE ashkinadzi.ext_currencies;
CREATE EXTERNAL TABLE  ashkinadzi.ext_currencies
(
    ID text,
	NumCode text,
	CharCode text,
	Nominal integer,
	Name text,
	Value numeric,
	Date date    
)
LOCATION ('pxf://user/s.ashkinadzi/currencies.csv?PROFILE=hdfs:text&HDFS_HOST=172.17.0.23&HDFS_PORT=8020')
FORMAT 'CSV' (header delimiter as ';');

-- security_transactions.csv
DROP EXTERNAL TABLE ashkinadzi.ext_security_transactions;
CREATE EXTERNAL TABLE  ashkinadzi.ext_security_transactions
(
    client_id BIGINT,
	security_id BIGINT,
	transaction_date timestamp,
	transaction_type text,
	quantity integer,
	currency text,
	price numeric
)
LOCATION ('pxf://user/s.ashkinadzi/security_transactions.csv?PROFILE=hdfs:text&HDFS_HOST=172.17.0.23&HDFS_PORT=8020')
FORMAT 'CSV' (header);

-- bank_transactions.csv
DROP EXTERNAL TABLE ashkinadzi.ext_bank_transactions;
CREATE EXTERNAL TABLE  ashkinadzi.ext_bank_transactions
(
    client_id BIGINT,
	transaction_id BIGINT,
	transaction_date timestamp,
	transaction_type text,
	account_number text,
	currency text,
	amount numeric
)
LOCATION ('pxf://user/s.ashkinadzi/bank_transactions.csv?PROFILE=hdfs:text&HDFS_HOST=172.17.0.23&HDFS_PORT=8020')
FORMAT 'CSV' (header);

grant all on ext_clients, ext_securities, ext_currencies, ext_security_transactions, ext_bank_transactions to wave15_user_a3;



