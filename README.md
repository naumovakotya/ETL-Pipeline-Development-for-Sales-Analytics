# 📊 Test_task: ETL-процесс в Kubernetes для аналитики продаж

## 🔹 Описание проекта
Описание дорабатывается, как и оформление данного проекта. Однако, в скором времени здесь будет самое актуальное состояние.
Проект разрабатывался для обработки и анализа данных продаж. Данные поступают в оперативную БД, затем через ETL переносятся в аналитическое хранилище (DWH), где агрегируются. Визуализация происходит через Power BI.

### 🔹 Стек технологий
- **ETL**: Apache Airflow
- **Хранилище**: PostgreSQL (DWH)
- **Оркестрация**: Kubernetes + Minikube
- **Контейнеризация**: Docker
- **BI**: Power BI
- **Управление БД**: DBeaver

---

## 🔹 Запуск проекта

### 📌 1. Установите Minikube и Kubectl  
- Инструкция: https://minikube.sigs.k8s.io/docs/start/

### 📌 2. Запустите Minikube  
```bash
minikube start
```

### 📌 3. Разверните инфраструктуру (БД, Airflow, ETL)  
```bash
./start_project.sh
```

### 📌 4. Прокиньте порты для PostgreSQL  
```bash
kubectl port-forward svc/postgres-operational 5434:5432 --namespace airflow-tt-v2 &
kubectl port-forward svc/postgres-analytics 5435:5432 --namespace airflow-tt-v2 &
```

### 📌 5. Открываем DBeaver и подключаемся:
- **Оперативная БД**: `localhost:5434`
- **Аналитическая БД**: `localhost:5435`

### 📌 6. Запуск DAG в Apache Airflow  
```bash
kubectl port-forward svc/airflow-webserver 8080:8080 --namespace airflow-tt-v2
```
Открываем **http://localhost:8080**, включаем DAG `dag_etl.py`.

### 📌 7. Анализ данных в Power BI  
- Готовый дашборд см. в `Test_task_Inovis.pbix`.

---

## 🔹 Скриншоты и результаты 📊
![Изображение WhatsApp 2025-02-06 в 18 53 07_6f453a38](https://github.com/user-attachments/assets/b3dd2361-39a4-4dcc-90d7-da532aa7fcec)

---

## 🔹 **Авторы**  
👩‍💻 Разработано в рамках тестового задания.  






