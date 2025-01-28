-- Создание функций по переносу данных из буферных таблиц в базовые
-------------------------------------------------------------------
-- clients

CREATE OR REPLACE FUNCTION ashkinadzi.gp_buff_to_base_transfer_clients() 
RETURNS void
AS $$
	BEGIN 
        -- обновление записей
		UPDATE gp_clients_base as base 
			SET client_name = buff.client_name, 
				client_email = buff.client_email, 
				client_phone = buff.client_phone, 
				client_address = buff.client_address, 
				updated_at = NOW() 
				--status = 'D'
		FROM gp_clients_buff as buff
		WHERE base.client_id = buff.client_id;
		
  		
		-- если не получилось обновить ни одной записи
		INSERT INTO gp_clients_base (client_id, client_name, client_email, client_phone, client_address) 
		SELECT client_id, client_name, client_email, client_phone, client_address FROM gp_clients_buff
			EXCEPT
		SELECT client_id, client_name, client_email, client_phone, client_address FROM gp_clients_base;

	END;
$$ LANGUAGE plpgsql set optimizer = OFF;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- securities

CREATE OR REPLACE FUNCTION ashkinadzi.gp_buff_to_base_transfer_securities() 
RETURNS void
AS $$
	BEGIN 
        -- обновление записей
		UPDATE gp_securities_base as base 
			SET security_name = buff.security_name, 
				security_type = buff.security_type, 
				market = buff.market, 
				updated_at = NOW() 
				--status = 'D'
		FROM gp_securities_buff as buff
		WHERE base.security_id = buff.security_id;
		
  		
		-- если не получилось обновить ни одной записи
		INSERT INTO gp_securities_base (security_id, security_name, security_type, market) 
		SELECT security_id, security_name, security_type, market FROM gp_securities_buff
			EXCEPT
		SELECT security_id, security_name, security_type, market FROM gp_securities_base;

	END;
$$ LANGUAGE plpgsql set optimizer = OFF;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- currencies

CREATE OR REPLACE FUNCTION ashkinadzi.gp_buff_to_base_transfer_currencies() 
RETURNS void
AS $$
	BEGIN 
		
			UPDATE gp_currencies_base as base 
				SET status = 'D'
			FROM gp_currencies_buff as buff
			WHERE base.Date = buff.Date and base.ID = buff.ID;

			INSERT INTO gp_currencies_base (ID, NumCode, CharCode, Nominal, Name, Value, Date, status) 
			SELECT buff.ID, buff.NumCode, buff.CharCode, buff.Nominal, buff.Name, buff.Value, buff.Date, 'a' FROM gp_currencies_buff as buff;

	END;
$$ LANGUAGE plpgsql set optimizer = OFF;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- security_transactions

CREATE OR REPLACE FUNCTION ashkinadzi.gp_buff_to_base_transfer_security_transactions() 
RETURNS void
AS $$
	BEGIN 
		
			UPDATE gp_security_transactions_base as base 
				SET updated_at = NOW(), 
					status = 'D'
			FROM gp_security_transactions_buff as buff
			WHERE base.client_id = buff.client_id and 
				  base.security_id = buff.security_id and 
				  base.transaction_date = buff.transaction_date;

			INSERT INTO gp_security_transactions_base (client_id, security_id, transaction_date, transaction_type, quantity, currency, price, security_transaction_id) 
			SELECT buff.client_id, buff.security_id, buff.transaction_date, buff.transaction_type, buff.quantity, buff.currency, buff.price, buff.security_transaction_id FROM gp_security_transactions_buff as buff;

	END;
$$ LANGUAGE plpgsql set optimizer = OFF;


------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- bank_transactions

CREATE OR REPLACE FUNCTION ashkinadzi.gp_buff_to_base_transfer_bank_transactions() 
RETURNS void
AS $$
	BEGIN 
		
			UPDATE gp_bank_transactions_base as base 
				SET updated_at = NOW(), 
					status = 'D'
			FROM gp_bank_transactions_buff as buff
			WHERE base.client_id = buff.client_id and 
				  base.transaction_id = buff.transaction_id and 
				  base.transaction_date = buff.transaction_date;

			INSERT INTO gp_bank_transactions_base (client_id, transaction_id, transaction_date, transaction_type, account_number, currency, amount) 
			SELECT buff.client_id, buff.transaction_id, buff.transaction_date, buff.transaction_type, buff.account_number, buff.currency, buff.amount FROM gp_bank_transactions_buff as buff;

	END;
$$ LANGUAGE plpgsql set optimizer = OFF;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- все настройки для текущей сессии
SELECT * FROM pg_settings; 


SHOW ALL; -- Просмотр всех текущих настроек
SHOW <parametr_name>; -- Просмотр значения конкретного параметра

SELECT * FROM pg_available_extensions(); -- получить список доступных для установки расширений в базе данных

SELECT extname FROM pg_extension; -- получить список расширений, установленных в базе данных

CREATE EXTENSION <имя_расширения> SCHEMA <имя_схемы>; -- добавить новые расширения

CREATE EXTENSION gpss;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select * from pg_attribute;

select * from pg_locks