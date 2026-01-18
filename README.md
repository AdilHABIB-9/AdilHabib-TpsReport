# TP2 — Modern Data Pipeline

*PostgreSQL → S3 → Snowflake → dbt → Airflow → Power BI*

## 📌 Description

This TP implements a full cloud-based Modern Data Stack pipeline using the **ShopStream** e-commerce scenario.

Technologies used:

* **PostgreSQL** — OLTP database
* **Amazon S3** — Data Lake (RAW Zone)
* **Snowflake** — Cloud Data Warehouse
* **dbt** — Transformations as SQL Models
* **Airflow** — Orchestration
* **Power BI** — BI Visualization

---

## 📁 Project Structure

```
TP2/
│
├── SQL (PostgreSQL schema)
├── scripts/
│   ├── generate_data.py
│   └── extraction_python.py
├── dbt/
├── airflow/
├── powerbi/
└── README.md
```

---

## 🧱 Step 1 — PostgreSQL (OLTP Layer)

Create the operational schema with 6 tables:

* users
* products
* orders
* order_items
* events
* crm_contacts

Run the provided SQL script to initialize the database.

---

## 🧪 Step 2 — Generate Sample Data

Python script used:

```
scripts/generate_data.py
```

It generates:

* Users
* Products
* Orders
* Order Items
* CRM contacts
* Events

All inserted directly into PostgreSQL.

---

## 🪣 Step 3 — S3 Data Lake (RAW Zone)

Using Python:

* Extract OLTP tables
* Save as CSV or Parquet
* Upload to your S3 bucket

Your S3 structure:

```
s3://shopstream-raw/
    users/
    products/
    orders/
    order_items/
    events/
    crm_contacts/
```

---

## ❄ Step 4 — Snowflake (STAGE + CORE)

### STAGE layer:

Load S3 data using:

```
COPY INTO stage.users
FROM @my_s3_stage/users
FILE_FORMAT = (TYPE = CSV ...)
```

### CORE layer:

Created using dbt transformations:

* Dimensional models
* Fact tables
* Business logic

---

## 🛠 Step 5 — dbt Transformations

Your dbt project includes:

* `staging` models
* `core` models
* `marts` models (business KPIs)

Run:

```
dbt run
dbt test
```

---

## ⏱ Step 6 — Orchestration with Airflow

The Airflow DAG triggers:

1. Extract from PostgreSQL
2. Upload to S3
3. Load Snowflake STAGE
4. Run dbt models
5. Refresh Power BI dataset

File examples:

* `dag_shopstream.py`
* `task_extract.py`
* `task_dbt.py`

---

## 📊 Step 7 — BI with Power BI

Final dashboard includes:

* Revenue by country
* Customer funnel
* CLV
* Top products
* Orders trends

---

## ✔ Deliverables 

* PostgreSQL SQL file
* Python extraction + generation scripts
* S3 folder structure
* Snowflake STAGE + CORE models
* dbt project
* Airflow DAG
* Power BI dashboard (.pbix)
* This README.md

---

