# 📊 Test_task: ETL Pipeline in Kubernetes for Sales Analytics  

## 🔹 Project Description  
This project was developed for processing and analyzing sales data. The data flows from an **operational database** to an **analytical data warehouse (DWH)** via an **ETL pipeline**, where it is aggregated. The final visualization is performed using **Power BI**.

### 🔹 Tech Stack  
- **ETL**: Apache Airflow  
- **Storage**: PostgreSQL (DWH)  
- **Orchestration**: Kubernetes + Minikube  
- **Containerization**: Docker  
- **BI Tool**: Power BI  
- **Database Management**: DBeaver  
- **Automation**: Bash scripts  

---

## 🔹 Project Structure  
The infrastructure consists of:  
1️⃣ **Operational Database** (stores raw transactions with timestamps).  
2️⃣ **MRR Layer** (stages all extracted data).  
3️⃣ **STG Layer** (aggregated and cleaned data).  
4️⃣ **DWH** (final structure for BI analytics).  
5️⃣ **Logging system** in `dwh_metadata.logs`.  
6️⃣ **High Water Mark** implemented for incremental updates.  

The project includes SQL scripts for creating databases and generating test data.

---

## 🔹 How to Run  

### 📌 1. Install Minikube and Kubectl  
- Follow the official guide: [Minikube Setup](https://minikube.sigs.k8s.io/docs/start/)  

### 📌 2. Start Minikube and Deploy the Infrastructure  
```bash
./start_project.sh
```
This script will:  
✔ Start Minikube  
✔ Deploy PostgreSQL, Airflow, and ETL processes  
✔ Forward necessary ports  
✔ Start Apache Airflow  

### 📌 3. Connect to Databases via DBeaver  
- **Operational DB**: `localhost:5434`  
- **Analytical DB**: `localhost:5435`  

### 📌 4. View the Power BI Dashboard  
- Сheck the screenshot below.  
![Изображение WhatsApp 2025-02-06 в 18 53 07_6f453a38](https://github.com/user-attachments/assets/b3dd2361-39a4-4dcc-90d7-da532aa7fcec)
---

## 🔹 Screenshots & Results 📊  

### ✅ Airflow ETL DAG  
![image](https://github.com/user-attachments/assets/411ef91b-9648-4d6a-b4b5-4f503f02f7a1)

### ✅ Airflow Backup DAG  
![image](https://github.com/user-attachments/assets/0c91e928-ede1-400e-98d4-9dda585d107d)

### ✅ DBeaver: Database Structure & Logs  
![image](https://github.com/user-attachments/assets/9e798642-6cca-45e4-968c-c63ec7b40b7e)


---

## 🔹 Authors  
Developed as part of a test task. 🚀  







