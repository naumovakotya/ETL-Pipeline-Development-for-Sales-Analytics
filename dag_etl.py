import json
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Подключение к БД
def connect_to_operational():
    """
    Возвращает соединение с оперативной базой 
    по Airflow Connection 'postgres_operational'.
    """
    hook = PostgresHook(postgres_conn_id="postgres_operational")
    return hook.get_conn()

def connect_to_analytics():
    """
    Возвращает соединение с аналитической базой (где находятся схемы MRR, STG, DWH и dwh_metadata)
    по Airflow Connection 'postgres_analytics'.
    """
    hook = PostgresHook(postgres_conn_id="postgres_analytics")
    return hook.get_conn()

# Система логирования
def log_event(package_name, status, message):
    """
    Записывает событие в таблицу логов.
    package_name – имя пакета/задачи (например, task_id),
    status – 'SUCCESS' или 'FAILURE',
    message – текст сообщения (например, описание ошибки).
    """
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="postgres_analytics")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dwh_metadata.etl_logs (package_name, log_time, status, message)
            VALUES (%s, NOW(), %s, %s);
        """, (package_name, status, message))
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

def etl_task(**kwargs):
    try:
        logging.info("Начало ETL-процесса: Operational → MRR → STG → DWH")
        
        # --- Шаг 1. Получение high water mark для mrr_fact_sales ---
        conn_anal = connect_to_analytics()
        cur_anal = conn_anal.cursor()
        cur_anal.execute("""
            SELECT last_update 
            FROM dwh_metadata.high_water_mark 
            WHERE table_name = 'mrr_fact_sales';
        """)
        row = cur_anal.fetchone()
        last_update = row[0] if row else datetime(2000, 1, 1) # Значение по умолчанию, если записи нет
        logging.info("High water mark (last_update): %s", last_update)
        
        # --- Шаг 2. Извлечение новых продаж из оперативной БД ---
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("""
            SELECT customer_id, product_id, qty, sale_date 
            FROM sales 
            WHERE sale_date > %s;
        """, (last_update,))
        new_sales = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        logging.info("Извлечено %d новых записей из оперативной БД", len(new_sales))
        
        # --- Шаг 3. Загрузка сырых данных продаж в MRR ---
        for sale in new_sales:
            sale_dict = {
                "customer_id": sale[0],
                "product_id": sale[1],
                "qty": sale[2],
                "sale_date": str(sale[3])
            }
            raw_json = json.dumps(sale_dict)
            cur_anal.execute("""
                INSERT INTO mrr.mrr_fact_sales (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_anal.commit()
        logging.info("Загружены сырые данные продаж в MRR (mrr.mrr_fact_sales)")
        
        # --- Шаг 4. Загрузка данных измерений (клиентов и продуктов) в MRR ---
        # Для клиентов
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("SELECT id, name, country FROM customers;")
        customers = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        cur_anal.execute("TRUNCATE TABLE mrr.mrr_dim_customers;")
        for cust in customers:
            cust_dict = {"id": cust[0], "name": cust[1], "country": cust[2]}
            raw_json = json.dumps(cust_dict)
            cur_anal.execute("""
                INSERT INTO mrr.mrr_dim_customers (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_anal.commit()
        logging.info("Загружены данные клиентов в MRR (mrr.mrr_dim_customers)")
        
        # Для продуктов
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("SELECT id, name, groupname FROM products;")
        products = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        cur_anal.execute("TRUNCATE TABLE mrr.mrr_dim_products;")
        for prod in products:
            prod_dict = {"id": prod[0], "name": prod[1], "groupname": prod[2]}
            raw_json = json.dumps(prod_dict)
            cur_anal.execute("""
                INSERT INTO mrr.mrr_dim_products (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_anal.commit()
        logging.info("Загружены данные продуктов в MRR (mrr.mrr_dim_products)")
        
        # --- Шаг 5. Трансформация данных из MRR в STG ---
        # Единый TRUNCATE для таблиц STG, чтобы избежать конфликтов внешних ключей
        cur_anal.execute("TRUNCATE TABLE stg.stg_fact_sales, stg.stg_dim_customers, stg.stg_dim_products CASCADE;")
        conn_anal.commit()
        
        # Трансформация измерений: клиентов
        cur_anal.execute("SELECT raw_data FROM mrr.mrr_dim_customers;")
        mrr_cust = cur_anal.fetchall()
        for row in mrr_cust:
            raw = row[0]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            cur_anal.execute("""
                INSERT INTO stg.stg_dim_customers (id, customer_name, country, load_date)
                VALUES (%s, %s, %s, NOW());
            """, (data["id"], data["name"], data["country"]))
        conn_anal.commit()
        logging.info("Данные клиентов преобразованы и загружены в STG (stg.stg_dim_customers)")
        
        # Трансформация измерений: продуктов
        cur_anal.execute("SELECT raw_data FROM mrr.mrr_dim_products;")
        mrr_prod = cur_anal.fetchall()
        for row in mrr_prod:
            raw = row[0]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            cur_anal.execute("""
                INSERT INTO stg.stg_dim_products (id, name, group_name, load_date)
                VALUES (%s, %s, %s, NOW());
            """, (data["id"], data["name"], data["groupname"]))
        conn_anal.commit()
        logging.info("Данные продуктов преобразованы и загружены в STG (stg.stg_dim_products)")
        
        # Трансформация фактов: продаж
        cur_anal.execute("SELECT raw_data FROM mrr.mrr_fact_sales;")
        mrr_sales = cur_anal.fetchall()
        for row in mrr_sales:
            raw = row[0]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            cur_anal.execute("""
                INSERT INTO stg.stg_fact_sales (customer_id, product_id, qty, sale_date, load_date)
                VALUES (%s, %s, %s, %s, NOW());
            """, (data["customer_id"], data["product_id"], data["qty"], data["sale_date"]))
        conn_anal.commit()
        logging.info("Данные продаж преобразованы и загружены в STG (stg.stg_fact_sales)")
        
        # --- Шаг 6. Загрузка данных из STG в DWH ---
        # Для устранения ошибок внешних ключей очищаем все таблицы DWH вместе
        cur_anal.execute("TRUNCATE TABLE dwh.dwh_fact_sales, dwh.dwh_dim_customers, dwh.dwh_dim_products CASCADE;")
        conn_anal.commit()
        
        # Сначала загружаем измерения в DWH
        cur_anal.execute("SELECT id, customer_name, country FROM stg.stg_dim_customers;")
        stg_cust = cur_anal.fetchall()
        for row in stg_cust:
            cur_anal.execute("""
                INSERT INTO dwh.dwh_dim_customers (id, customer_name, country)
                VALUES (%s, %s, %s);
            """, row)
            
        cur_anal.execute("SELECT id, name, group_name FROM stg.stg_dim_products;")
        stg_prod = cur_anal.fetchall()
        for row in stg_prod:
            cur_anal.execute("""
                INSERT INTO dwh.dwh_dim_products (id, name, group_name)
                VALUES (%s, %s, %s);
            """, row)
        conn_anal.commit()
        
        # Затем загружаем факты в DWH
        cur_anal.execute("SELECT customer_id, product_id, qty, sale_date FROM stg.stg_fact_sales;")
        stg_sales = cur_anal.fetchall()
        for row in stg_sales:
            cur_anal.execute("""
                INSERT INTO dwh.dwh_fact_sales (customer_id, product_id, qty, sale_date)
                VALUES (%s, %s, %s, %s);
            """, row)
        conn_anal.commit()
        logging.info("Данные загружены в DWH (dwh.dwh_fact_sales, dwh.dwh_dim_customers, dwh.dwh_dim_products)")
        
        # --- Шаг 7. Обновление high water mark ---
        if new_sales:
            new_last_update = max(sale[3] for sale in new_sales)
            cur_anal.execute("""
                UPDATE dwh_metadata.high_water_mark
                SET last_update = %s
                WHERE table_name = 'mrr_fact_sales';
            """, (new_last_update,))
            conn_anal.commit()
            logging.info("Обновлён high water mark: %s", new_last_update)
        else:
            logging.info("Новых данных нет, high water mark не изменён.")
        
        cur_anal.close()
        conn_anal.close()
        logging.info("ETL-процесс успешно завершён.")
        
    except Exception as e:
        logging.error("Ошибка в ETL-процессе", exc_info=True)
        raise

with DAG(
    'etl_dag',
    default_args=default_args,
    description='ETL: Operational → MRR → STG → DWH с использованием high water mark',
    schedule_interval='@hourly',
    catchup=False
) as dag:
    etl = PythonOperator(
        task_id='etl_task',
        python_callable=etl_task,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag
    )
    trigger_backup = TriggerDagRunOperator(
        task_id="trigger_backup_dag",
        trigger_dag_id="backup_dag",  
        wait_for_completion=False, 
        dag=dag
    )
    
    etl >> trigger_backup