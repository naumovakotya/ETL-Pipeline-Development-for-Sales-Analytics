from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import subprocess
import os
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 5),
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
}

# Система логирования
def log_event(process_name, status, message):
    """
    Записывает событие в таблицу логов.
    process_name – имя пакета/задачи (например, task_id),
    status – 'SUCCESS' или 'FAILURE',
    message – текст сообщения (например, описание ошибки).
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_dwh")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dwh_metadata.logs (process_name, log_time, status, message)
            VALUES (%s, NOW(), %s, %s);
        """, (process_name, status, message))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as log_ex:
        logging.error("Не удалось записать лог: %s", str(log_ex))

# Коллбэки для логирования
def on_success_callback(context):
    task_id = context['task_instance'].task_id
    log_event(task_id, "SUCCESS", "Задача выполнена успешно.")

def on_failure_callback(context):
    task_id = context['task_instance'].task_id
    exception = context.get('exception')
    log_event(task_id, "FAILURE", f"Ошибка: {str(exception)}")

def backup_schema(schema_name, postgres_conn_id="postgres_dwh", backup_dir="/backup"):
    """
    Выполняет резервное копирование указанной схемы с помощью pg_dump.
    Файл сохраняется в директории backup_dir с именем вида: schema_YYYYMMDDHHMMSS.sql
    """
    os.makedirs(backup_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    backup_file = os.path.join(backup_dir, f"{schema_name}_{timestamp}.sql")

    # Получаем параметры подключения из Airflow Connections
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn_obj = hook.get_connection(postgres_conn_id)

    pg_host = conn_obj.host
    pg_port = conn_obj.port
    pg_user = conn_obj.login
    pg_password = conn_obj.password
    pg_db = conn_obj.schema  # База данных (test_analytics)

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
        log_event(f"backup_{schema_name}", "SUCCESS", f"Backup completed: {backup_file}")
        return backup_file

    except Exception as e:
        logging.error(f"Ошибка при резервном копировании схемы {schema_name}: {str(e)}")
        # Записываем ошибку в лог
        log_event(f"backup_{schema_name}", "FAILURE", str(e))


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
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        op_kwargs={'schema_name': 'public'}
    )
    backup_stg = PythonOperator(
        task_id='backup_stg',
        python_callable=backup_task_func,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        op_kwargs={'schema_name': 'public'}
    )
    backup_dwh = PythonOperator(
        task_id='backup_dwh',
        python_callable=backup_task_func,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        op_kwargs={'schema_name': 'public'}
    )
    
    backup_dwh_metadata = PythonOperator(
        task_id='backup_dwh_metadata',
        python_callable=backup_task_func,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        op_kwargs={'schema_name': 'dwh_metadata'}
    )

    backup_mrr >> backup_stg >> backup_dwh >> backup_dwh_metadata
