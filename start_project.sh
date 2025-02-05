#!/bin/bash

# === Переменные ===
MINIKUBE_DRIVER="docker"
PROJECT_PATH="/mnt/c/Users/Kotya/Desktop/Job_search/Test_tasks/Inovis/test_task_inovis"
MANIFESTS_PATH="$PROJECT_PATH/manifests"
AIRFLOW_ENV_PATH="$PROJECT_PATH/airflow_env"
DAG_NAME="etl_prosess_dag"

# === Шаг 1: Запуск Minikube ===
echo "Запуск Minikube..."
minikube start --driver=$MINIKUBE_DRIVER

# === Шаг 2: Применение манифестов ===
echo "Применение манифестов Kubernetes..."
kubectl apply -f "$MANIFESTS_PATH/postgres-services.yaml"
kubectl apply -f "$MANIFESTS_PATH/postgres-analytics-db.yaml"
kubectl apply -f "$MANIFESTS_PATH/postgres-operational-db.yaml"
kubectl apply -f "$MANIFESTS_PATH/postgres-secrets.yaml"
echo "Ожидание запуска подов PostgreSQL..."
sleep 10  # Даем время контейнерам запуститься

# === Шаг 3: Прокидывание портов для PostgreSQL ===
echo "Настройка port-forward для баз данных..."
kubectl port-forward pod/postgres-operational-db-0 5432:5432 > operational_port.log 2>&1 &
disown
kubectl port-forward pod/postgres-analytics-db-0 5433:5432 > analytics_port.log 2>&1 &
disown
echo "Порты прокинуты в фоне. Проверьте доступ к базам данных через DBeaver."

# === Шаг 4: Настройка Airflow ===
cd "$AIRFLOW_ENV_PATH" || exit 1
source bin/activate
airflow db migrate
nohup airflow webserver -p 8080 > airflow_webserver.log 2>&1 &
disown
nohup airflow scheduler > airflow_scheduler.log 2>&1 &
disown
echo "Airflow запущен. Доступен по адресу: http://localhost:8080"
sleep 10 # Даем время Airflow запуститься

# === Шаг 5: Установка библиотек после активации окружения Airflow ===
echo "Настройка окружения Airflow..."
cd "$AIRFLOW_ENV_PATH"
source bin/activate

echo "Установка необходимых библиотек..."
# Список необходимых библиотек
REQUIRED_LIBRARIES=(
    "psycopg2-binary"
    "pandas"
    "apache-airflow[postgres,kubernetes]"
    "numpy"
)
# Функция для установки библиотек
install_library() {
    local LIBRARY=$1
    echo "Устанавливаю $LIBRARY..."
    if pip install "$LIBRARY"; then
        echo "$LIBRARY успешно установлена."
    else
        echo "Ошибка при установке $LIBRARY." >&2
        exit 1
    fi
}
# Проверка на наличие библиотек
for LIBRARY in "${REQUIRED_LIBRARIES[@]}"; do
    if ! pip freeze | grep -q "^$(echo $LIBRARY | cut -d '[' -f 1)="; then
        install_library "$LIBRARY"
    else
        echo "$LIBRARY уже установлена."
    fi
done

# # === Шаг 6: Запуск DAG ===
# echo "Запуск DAG $DAG_NAME..."
# airflow dags trigger --conf '{}' $DAG_NAME || echo "DAG $DAG_NAME не найден или уже запущен."

echo "THE END!"