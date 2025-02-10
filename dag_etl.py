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
    'start_date': datetime(2025, 2, 5),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Подключения к разным инстансам
def connect_to_operational():
    hook = PostgresHook(postgres_conn_id="postgres_operational")
    return hook.get_conn()

def connect_to_mrr():
    hook = PostgresHook(postgres_conn_id="postgres_mrr")
    return hook.get_conn()

def connect_to_stg():
    hook = PostgresHook(postgres_conn_id="postgres_stg")
    return hook.get_conn()

def connect_to_dwh():
    hook = PostgresHook(postgres_conn_id="postgres_dwh")
    return hook.get_conn()

# Система логирования: пишем в таблицу dwh_metadata.logs в базе DWH (connection postgres_dwh)
def log_event(process_name, status, message):
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
        
        # --- Шаг 1. Получение high water mark из базы dwh (схема dwh_metadata) ---
        conn_dwh = connect_to_dwh()
        cur_dwh = conn_dwh.cursor()
        cur_dwh.execute("""
            SELECT last_update 
            FROM dwh_metadata.high_water_mark 
            WHERE table_name = 'mrr_fact_sales';
        """)
        row = cur_dwh.fetchone()
        last_update = row[0] if row else datetime(2000, 1, 1)
        logging.info("High water mark (last_update): %s", last_update)
        cur_dwh.close()
        conn_dwh.close()
        
        # --- Шаг 2. Извлечение новых продаж из операционной базы ---
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("""
            SELECT id, customer_id, product_id, qty, sale_date 
            FROM sales 
            WHERE sale_date > %s;
        """, (last_update,))
        new_sales = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        logging.info("Извлечено %d новых записей из оперативной БД", len(new_sales))
        
        # --- Шаг 3. Загрузка сырых данных продаж в базу mrr ---
        conn_mrr = connect_to_mrr()
        cur_mrr = conn_mrr.cursor()
        for sale in new_sales:
            sale_dict = {
                "source_id": sale[0],
                "customer_id": sale[1],
                "product_id": sale[2],
                "qty": sale[3],
                "sale_date": str(sale[4])
            }
            raw_json = json.dumps(sale_dict)
            cur_mrr.execute("""
                INSERT INTO mrr_fact_sales (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_mrr.commit()
        logging.info("Загружены сырые данные продаж в MRR (mrr_fact_sales)")
        
        # --- Шаг 4. Загрузка измерений в mrr ---
        # Для клиентов
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("SELECT id, name, country FROM customers;")
        customers = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        cur_mrr.execute("TRUNCATE TABLE mrr_dim_customers;")
        for cust in customers:
            cust_dict = {"id": cust[0], "name": cust[1], "country": cust[2]}
            raw_json = json.dumps(cust_dict)
            # Здесь добавляем source_id; предположим, что source_id равен id
            cur_mrr.execute("""
                INSERT INTO mrr_dim_customers (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_mrr.commit()
        logging.info("Загружены данные клиентов в MRR (mrr_dim_customers)")
        
        # Для продуктов
        conn_oper = connect_to_operational()
        cur_oper = conn_oper.cursor()
        cur_oper.execute("SELECT id, name, groupname FROM products;")
        products = cur_oper.fetchall()
        cur_oper.close()
        conn_oper.close()
        cur_mrr.execute("TRUNCATE TABLE mrr_dim_products;")
        for prod in products:
            prod_dict = {"id": prod[0], "name": prod[1], "groupname": prod[2]}
            raw_json = json.dumps(prod_dict)
            cur_mrr.execute("""
                INSERT INTO mrr_dim_products (raw_data, load_date)
                VALUES (%s, NOW());
            """, (raw_json,))
        conn_mrr.commit()
        logging.info("Загружены данные продуктов в MRR (mrr_dim_products)")
        cur_mrr.close()
        conn_mrr.close()
        
        # --- Шаг 5. Трансформация данных из mrr в stg ---
        # Подключаемся к базам mrr и stg
        conn_mrr = connect_to_mrr()
        cur_mrr = conn_mrr.cursor()
        conn_stg = connect_to_stg()
        cur_stg = conn_stg.cursor()
        
        # Единый TRUNCATE для таблиц stg (с CASCADE, чтобы избежать ошибок внешних ключей)
        cur_stg.execute("TRUNCATE TABLE stg_fact_sales, stg_dim_customers, stg_dim_products CASCADE;")
        conn_stg.commit()
        
        # Трансформация измерений: клиентов
        cur_mrr.execute("SELECT raw_data FROM mrr_dim_customers;")
        mrr_cust = cur_mrr.fetchall()
        for row in mrr_cust:
            raw = row[0]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            # Теперь предполагается, что таблица stg_dim_customers имеет колонки:
            # id, source_id, customer_name, country, load_date
            cur_stg.execute("""
                INSERT INTO stg_dim_customers (id, source_id, customer_name, country, load_date)
                VALUES (%s, %s, %s, %s, NOW());
            """, (data["id"], data["id"], data["name"], data["country"]))
        conn_stg.commit()
        logging.info("Данные клиентов преобразованы и загружены в STG (stg_dim_customers)")
        
        # Трансформация измерений: продуктов
        cur_mrr.execute("SELECT raw_data FROM mrr_dim_products;")
        mrr_prod = cur_mrr.fetchall()
        for row in mrr_prod:
            raw = row[0]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            # Предполагаем, что таблица stg_dim_products имеет колонки:
            # id, source_id, name, group_name, load_date,
            # и source_id не может быть NULL.
            cur_stg.execute("""
                INSERT INTO stg_dim_products (id, source_id, name, group_name, load_date)
                VALUES (%s, %s, %s, %s, NOW());
            """, (data["id"], data["id"], data["name"], data["groupname"]))
        conn_stg.commit()
        logging.info("Данные продуктов преобразованы и загружены в STG (stg_dim_products)")
        
        # Трансформация фактов: продаж
        cur_mrr.execute("SELECT id, raw_data FROM mrr_fact_sales;")
        mrr_sales = cur_mrr.fetchall()
        for row in mrr_sales:
            mrr_id = row[0]
            raw = row[1]
            data = raw if isinstance(raw, dict) else json.loads(raw)
            # Если ключ "source_id" отсутствует, подставляем customer_id (опционально)
            source_id = data.get("source_id", data["customer_id"])
            cur_stg.execute("""
                INSERT INTO stg_fact_sales (source_id, mrr_id, customer_id, product_id, qty, sale_date, load_date)
                VALUES (%s, %s, %s, %s, %s, %s, NOW());
            """, (source_id, mrr_id, data["customer_id"], data["product_id"], data["qty"], data["sale_date"]))
        conn_stg.commit()
        logging.info("Данные продаж преобразованы и загружены в STG (stg_fact_sales)")  

        cur_mrr.close()
        conn_mrr.close()
        cur_stg.close()
        conn_stg.close()
        
        # --- Шаг 6. Загрузка данных из stg в dwh ---
        conn_dwh = connect_to_dwh()
        cur_dwh = conn_dwh.cursor()
        # Очищаем итоговые таблицы (в базе dwh находятся в public)
        cur_dwh.execute("TRUNCATE TABLE dwh_fact_sales, dwh_dim_customers, dwh_dim_products CASCADE;")
        conn_dwh.commit()
        
        # Сначала загружаем измерения из stg в dwh
        # Для клиентов
        conn_stg = connect_to_stg()
        cur_stg = conn_stg.cursor()
        cur_stg.execute("SELECT id, source_id, customer_name, country FROM stg_dim_customers;")
        stg_cust = cur_stg.fetchall()
        for row in stg_cust:
            cur_dwh.execute("""
                INSERT INTO dwh_dim_customers (id, source_id, customer_name, country)
                VALUES (%s, %s, %s, %s);
            """, row)
        # Для продуктов
        cur_stg.execute("SELECT id, source_id, name, group_name FROM stg_dim_products;")
        stg_prod = cur_stg.fetchall()
        for row in stg_prod:
            cur_dwh.execute("""
                INSERT INTO dwh_dim_products (id, source_id, name, group_name)
                VALUES (%s, %s, %s, %s);
            """, row)
        conn_dwh.commit()
        
        # Затем загружаем факты из stg в dwh
        cur_stg.execute("SELECT source_id, customer_id, product_id, qty, sale_date FROM stg_fact_sales;")
        stg_sales = cur_stg.fetchall()
        for row in stg_sales:
            cur_dwh.execute("""
                INSERT INTO dwh_fact_sales (source_id, customer_id, product_id, qty, sale_date)
                VALUES (%s, %s, %s, %s, %s);
            """, row)
        conn_dwh.commit()
        logging.info("Данные продаж загружены в DWH (dwh_fact_sales, dwh_dim_customers, dwh_dim_products)")
        
        cur_stg.close()
        conn_stg.close()
        cur_dwh.close()
        conn_dwh.close()
        
        # --- Шаг 7. Обновление high water mark в dwh ---
        if new_sales:
            new_last_update = max(sale[4] for sale in new_sales)
            # Если new_last_update не является объектом datetime, преобразуем его
            if not isinstance(new_last_update, datetime):
                new_last_update = datetime.fromtimestamp(new_last_update)
            conn_dwh = connect_to_dwh()
            cur_dwh = conn_dwh.cursor()
            cur_dwh.execute("""
                UPDATE dwh_metadata.high_water_mark
                SET last_update = %s
                WHERE table_name = 'mrr_fact_sales';
            """, (new_last_update,))
            conn_dwh.commit()
            cur_dwh.close()
            conn_dwh.close()
            logging.info("Обновлён high water mark: %s", new_last_update)
        else:
            logging.info("Новых данных нет, high water mark не изменён.")
        
        log_event("etl_dag", "SUCCESS", "ETL-процесс успешно завершён.")
        logging.info("ETL-процесс успешно завершён.")
        
    except Exception as e:
        log_event("etl_dag", "FAILURE", f"Ошибка в ETL-процессе: {str(e)}")
        logging.error("Ошибка в ETL-процессе", exc_info=True)
        raise e

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
        on_failure_callback=on_failure_callback
    )
    
    trigger_backup = TriggerDagRunOperator(
        task_id="trigger_backup_dag",
        trigger_dag_id="backup_dag",  
        wait_for_completion=False
    )
    
    etl >> trigger_backup

