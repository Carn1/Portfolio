create or replace procedure ashkinadzi.buff_to_base_transfer_clients() as $$
begin 

	merge into ashkinadzi.clients_base 
	using ashkinadzi.clients_buff
	on clients_base.client_id = clients_buff.client_id
	when not matched then
		insert (client_id, client_name, client_email, client_phone, client_address, updated_at) values (clients_buff.client_id, clients_buff.client_name, clients_buff.client_email, clients_buff.client_phone, clients_buff.client_address, now())
	when matched then 
		update set client_name = clients_buff.client_name, client_email = clients_buff.client_email, client_phone = clients_buff.client_phone, client_address = clients_buff.client_address, updated_at = now();
	 
end;
$$ language plpgsql;



create or replace procedure ashkinadzi.buff_to_base_transfer_securities() as $$
begin 

	merge into ashkinadzi.securities_base 
	using ashkinadzi.securities_buff
	on securities_base.security_id = securities_buff.security_id
	when not matched then
		insert (security_id, security_name, security_type, market, updated_at) values (securities_buff.security_id, securities_buff.security_name, securities_buff.security_type, securities_buff.market, now())
	when matched then 
		update set security_name = securities_buff.security_name, security_type = securities_buff.security_type, market = securities_buff.market, updated_at = now();
	 
end;
$$ language plpgsql;



create or replace procedure ashkinadzi.buff_to_base_transfer_security_transactions() as $$
begin 

	merge into ashkinadzi.security_transactions_base 
	using ashkinadzi.security_transactions_buff
	on security_transactions_base.client_id = security_transactions_buff.client_id and security_transactions_base.security_id = security_transactions_buff.security_id and security_transactions_base.transaction_date = security_transactions_buff.transaction_date
	when matched then 
		update set status = 'D', updated_at = now();
	
	merge into ashkinadzi.security_transactions_base 
	using ashkinadzi.security_transactions_buff
	on security_transactions_base.client_id = security_transactions_buff.client_id and security_transactions_base.security_id = security_transactions_buff.security_id and security_transactions_base.transaction_date = security_transactions_buff.transaction_date
	when not matched then 
		insert (client_id, security_id, transaction_date, transaction_type, quantity, currency, price, updated_at) values (security_transactions_buff.client_id, security_transactions_buff.security_id, security_transactions_buff.transaction_date, security_transactions_buff.transaction_type, security_transactions_buff.quantity, security_transactions_buff.currency, security_transactions_buff.price, now());
	 
end;
$$ language plpgsql;



create or replace procedure ashkinadzi.buff_to_base_transfer_transactions() as $$
begin 

	merge into ashkinadzi.transactions_base 
	using ashkinadzi.transactions_buff
	on transactions_base.client_id = transactions_buff.client_id and transactions_base.transaction_id = transactions_buff.transaction_id and transactions_base.transaction_date = transactions_buff.transaction_date
	when matched then 
		update set status = 'D', updated_at = now();
	
	merge into ashkinadzi.transactions_base 
	using ashkinadzi.transactions_buff
	on transactions_base.client_id = transactions_buff.client_id and transactions_base.transaction_id = transactions_buff.transaction_id and transactions_base.transaction_date = transactions_buff.transaction_date
	when not matched then 
		insert (client_id, transaction_id, transaction_date, transaction_type, account_number, currency, amount, updated_at) values (transactions_buff.client_id, transactions_buff.transaction_id, transactions_buff.transaction_date, transactions_buff.transaction_type, transactions_buff.account_number, transactions_buff.currency, transactions_buff.amount, now());
	 
end;
$$ language plpgsql;



create or replace procedure ashkinadzi.buff_to_base_transfer_currencies() as $$
begin 

	merge into ashkinadzi.currencies_base 
	using ashkinadzi.currencies_buff
	on currencies_base.date = currencies_buff.date
	when matched then 
		update set status = 'D', updated_at = now();
	
	merge into ashkinadzi.currencies_base 
	using ashkinadzi.currencies_buff
	on currencies_base.date = currencies_buff.date
	when not matched then 
		insert (id, numcode, charcode, nominal, "name", value, "date", updated_at) values (currencies_buff.id, currencies_buff.numcode, currencies_buff.charcode, currencies_buff.nominal, currencies_buff."name", currencies_buff.value, currencies_buff."date", now());

end;
$$ language plpgsql;


