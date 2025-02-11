#!/bin/bash

# === Переменные ===
MINIKUBE_DRIVER="docker"
PROJECT_PATH="/mnt/c/Users/Kotya/Desktop/Job_search/Test_tasks/Inovis/tt_inovis_v2"
MANIFESTS_PATH="$PROJECT_PATH/manifests"
AIRFLOW_ENV_PATH="$PROJECT_PATH/airflow_env"

# === Шаг 1: Запуск Minikube ===
echo "Запуск Minikube..."
minikube start --driver=$MINIKUBE_DRIVER

# === Создание namespace ===
kubectl create namespace airflow-tt-v2

# === Шаг 2: Применение манифестов ===
# Список манифестов для применения
FILES=(
  "postgres-services.yaml"
  "postgres-secrets.yaml"
  "postgres-operational-db.yaml"
  "postgres-analytics-db.yaml"
)

echo "Применение манифестов Kubernetes..."
# Применение каждого манифеста
for file in "${FILES[@]}"; do
  echo "Устанавливаю $file..."
  kubectl apply -f "$MANIFESTS_PATH/$file"
  if [ $? -ne 0 ]; then
    echo "Ошибка при установке $file."
    exit 1
  fi
done
echo "Ожидание запуска подов PostgreSQL и Airflow..."
sleep 10  # Даем время контейнерам запуститься

# === Шаг 3: Прокидывание портов для PostgreSQL ===
kubectl port-forward pod/postgres-operational-db-0 5434:5432 --namespace airflow-tt-v2 > operational_port.log 2>&1 &
disown
kubectl port-forward pod/postgres-analytics-db-0 5435:5432 --namespace airflow-tt-v2 > analytics_port.log 2>&1 &
disown
echo "Порты прокинуты в фоне. Проверьте доступ к базам данных через DBeaver."

# # === Шаг 4: Настройка Airflow ===
cd "$AIRFLOW_ENV_PATH" || exit 1
source bin/activate
airflow db migrate
nohup airflow webserver -p 8081 > airflow_webserver.log 2>&1 &
disown
nohup airflow scheduler > airflow_scheduler.log 2>&1 &
disown
echo "Airflow запущен. Доступен по адресу: http://localhost:8081"
sleep 10 # Даем время Airflow запуститься

# # === Шаг 5: Установка библиотек после активации окружения Airflow ===
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
    "apache-airflow-providers-postgres"
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

echo "THE END!"