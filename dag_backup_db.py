from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import subprocess
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def backup_schema(schema_name, postgres_conn_id="postgres_analytics", backup_dir="/backup"):
    """
    Выполняет резервное копирование указанной схемы с помощью pg_dump.
    Файл сохраняется в директории backup_dir с именем вида: schema_YYYYMMDDHHMMSS.sql
    """
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_file = os.path.join(backup_dir, f"{schema_name}_{timestamp}.sql")

    # Получаем параметры подключения из Airflow Connections
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_connection(postgres_conn_id)

    pg_host = conn.host
    pg_port = conn.port
    pg_user = conn.login
    pg_password = conn.password
    pg_db = conn.schema  # База данных (test_analytics)

    # Используем параметр -n (namespace), чтобы сделать дамп только схемы
    cmd = f'PGPASSWORD="{pg_password}" pg_dump -h {pg_host} -p {pg_port} -U {pg_user} -d {pg_db} -n {schema_name} -F c -f {backup_file}'
    
    logging.info(f"Выполняется backup для схемы {schema_name}: {cmd}")
    
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode != 0:
        raise Exception(f"Ошибка при резервном копировании схемы {schema_name}: {result.stderr}")
    
    return backup_file

def backup_task_func(schema_name, **kwargs):
    """
    Запускает резервное копирование схемы базы данных и логирует результат.
    """
    backup_dir = os.path.expanduser("~/backup")
    
    try:
        backup_file = backup_schema(schema_name, backup_dir=backup_dir)

        # Записываем успех в лог
        hook = PostgresHook(postgres_conn_id="postgres_analytics")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dwh_metadata.etl_logs (package_name, log_time, status, message)
            VALUES (%s, NOW(), %s, %s);
        """, (f"backup_{schema_name}", "SUCCESS", f"Backup completed: {backup_file}"))
        conn.commit()
        cur.close()
        conn.close()

        return backup_file

    except Exception as e:
        logging.error(f"Ошибка при резервном копировании схемы {schema_name}: {str(e)}")

        # Записываем ошибку в лог
        hook = PostgresHook(postgres_conn_id="postgres_analytics")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dwh_metadata.etl_logs (package_name, log_time, status, message)
            VALUES (%s, NOW(), %s, %s);
        """, (f"backup_{schema_name}", "FAILURE", str(e)))
        conn.commit()
        cur.close()
        conn.close()

with DAG(
    'backup_dag',
    default_args=default_args,
    description='Backup для схем mrr, stg, dwh в базе test_analytics',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    backup_mrr = PythonOperator(
        task_id='backup_mrr',
        python_callable=backup_task_func,
        op_kwargs={'schema_name': 'mrr'}
    )
    backup_stg = PythonOperator(
        task_id='backup_stg',
        python_callable=backup_task_func,
        op_kwargs={'schema_name': 'stg'}
    )
    backup_dwh = PythonOperator(
        task_id='backup_dwh',
        python_callable=backup_task_func,
        op_kwargs={'schema_name': 'dwh'}
    )

    backup_mrr >> backup_stg >> backup_dwh
