from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine
import pandas as pd

# Definir las conexiones (asegúrate de que estas estén configuradas en Airflow)
SRC_CONN_ID = 'postgres1_conn_id'
DST_CONN_ID = 'postgres2_conn_id'

def transfer_data():
    src_engine = create_engine(f'postgresql+psycopg2://user:password@host1:5432/dbname1')
    dst_engine = create_engine(f'postgresql+psycopg2://user:password@host2:5433/dbname2')
    
    # Consulta para seleccionar nuevos registros (ajusta la condición según tu lógica de incremento)
    query = "SELECT * FROM transactions WHERE created_at > (SELECT COALESCE(MAX(created_at), '1970-01-01') FROM transactions)"
    
    new_data = pd.read_sql(query, src_engine)
    
    # Asegúrate de que existe nueva data antes de intentar insertar
    if not new_data.empty:
        new_data.to_sql('transactions', dst_engine, if_exists='append', index=False)

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transfer_data_incrementally',
    default_args=default_args,
    description='Transfiere datos incrementales de una DB a otra cada hora',
    schedule_interval=timedelta(hours=1),
)

t1 = PythonOperator(
    task_id='transfer_data',
    python_callable=transfer_data,
    dag=dag,
)

t1
