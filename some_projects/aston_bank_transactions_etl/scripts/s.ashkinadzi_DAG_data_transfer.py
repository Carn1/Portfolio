from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2

default_args = {
	'owner': 's.ashkinadzi',
	'depends_on_past': False,
	'start_date': datetime(2025, 1, 13),
	#'email': ['airflow@example.com'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 3,
	'retry_delay': timedelta(minutes=15),
	'schedule_interval': '@daily',
}

DAG_data_transfer = DAG('s.ashkinadzi_DAG_data_transfer', catchup=False, default_args=default_args)

def buffer_tables_truncating():
#Предварительная очистка буферных таблиц в GreenPlum
    with psycopg2.connect(
        host='###',
        user='gpadmin',
        password='###',
        dbname='wave15_team_a',
        port=5432) as conn:
        with conn.cursor() as cur:
            cur.execute('SET search_path TO ashkinadzi')
            cur.execute('TRUNCATE gp_clients_buff, gp_securities_buff, gp_currencies_buff, gp_security_transactions_buff, gp_bank_transactions_buff RESTART IDENTITY CASCADE')


def external_to_internal_buffer():
#Перенос данных из внешних таблиц во внутренние буферные
    with psycopg2.connect(
        host='###',
        user='gpadmin',
        password='###',
        dbname='wave15_team_a',
        port=5432) as conn:
        with conn.cursor() as cur:
            cur.execute('SET search_path TO ashkinadzi')
            cur.execute('INSERT INTO gp_clients_buff SELECT * FROM ext_clients')
            cur.execute('INSERT INTO gp_securities_buff SELECT * FROM ext_securities')
            cur.execute('INSERT INTO gp_currencies_buff SELECT * FROM ext_currencies')
            cur.execute('INSERT INTO gp_security_transactions_buff SELECT * FROM ext_security_transactions')
            cur.execute('INSERT INTO gp_bank_transactions_buff SELECT * FROM ext_bank_transactions')


def buffer_to_base():
#Перенос данных из буферных в базовые таблицы
    with psycopg2.connect(
        host='###',
        user='gpadmin',
        password='###',
        dbname='wave15_team_a',
        port=5432) as conn:
        with conn.cursor() as cur:
            cur.execute('SET search_path TO ashkinadzi')
            cur.execute('SELECT gp_buff_to_base_transfer_clients()')
            cur.execute('SELECT gp_buff_to_base_transfer_securities()')
            cur.execute('SELECT gp_buff_to_base_transfer_currencies()')
            cur.execute('SELECT gp_buff_to_base_transfer_security_transactions()')
            cur.execute('SELECT gp_buff_to_base_transfer_bank_transactions()')    


### Создание тасок

t1 = PythonOperator(task_id = 'buff_tables_truncating',
                    python_callable=buffer_tables_truncating,
                    dag=DAG_data_transfer,
                    )

t2 = PythonOperator(task_id='ext_to_int_buffer_transfer',
                    python_callable=external_to_internal_buffer,
                    dag=DAG_data_transfer,
                    )

t3 = PythonOperator(task_id='buff_to_base_transfer',
                    python_callable=buffer_to_base,
                    dag=DAG_data_transfer,
                    )

### Создание пайплайна

t1 >> t2 >> t3