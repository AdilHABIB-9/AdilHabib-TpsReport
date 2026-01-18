# TP1 — Pipeline OLTP → Pentaho ETL → Data Warehouse → Power BI

## 📌 Description

This TP covers the complete Business Intelligence workflow for an e-commerce use case (**TechStore**).
It starts from an OLTP database and ends with an analytical dashboard in **Power BI**.

The objectives are:

* Understand the difference between **OLTP** (transactional) and **OLAP** (analytical) systems
* Extract and transform data using **Pentaho Data Integration (PDI)**
* Build a **Data Warehouse** (star schema)
* Load and analyze the data in **Power BI**

---

## 📁 Project Structure

```
TP1/
│
├── Script SQL OLTP
├── Script SQL DWH
├── Pentaho Transformations (.ktr)
├── Pentaho Jobs (.kjb)
├── Generated CSV files
└── README.md
```

---

## 🧱 Step 1 — Build the OLTP Database

### Database: `ventes_oltp`

Create the 4 OLTP tables:

* **clients**
* **produits**
* **commandes**
* **lignes_commandes**

These tables represent all the transactional operations of TechStore.

### ✔ You must run the script:

`_mysql_oltp_creation.sql`

---

## 📊 Step 2 — Generate Synthetic Data

Python is used to generate:

* 10,000 clients
* 500 products
* 20,000 commandes
* 100,000 lignes_commandes

Run:

```
python generate_data.py
```

This will produce the CSV files used later by Pentaho.

---

## 🔄 Step 3 — ETL with Pentaho PDI

The ETL process includes:

### **1. Extract**

* Read CSV files
* Clean data
* Convert types

### **2. Transform**

* Create surrogate keys
* Apply business rules
* Denormalize to fit the star schema

### **3. Load**

* Insert data into the Data Warehouse (`ventes_dwh`)

Your main files:

* `transform_clients.ktr`
* `transform_produits.ktr`
* `transform_commandes.ktr`
* `job_global.kjb`

---

## 🏛 Step 4 — Build the Data Warehouse (DWH)

Schema used: **Star Schema**

### Dimensions:

* dim_client
* dim_produit
* dim_date

### Fact table:

* fact_ventes

This structure enables fast analytical queries.

---

## 📈 Step 5 — Reporting in Power BI

Connect Power BI to the `ventes_dwh` database.

Create dashboards that show:

* Top 10 products
* Sales evolution by month
* Sales by city
* Sales by category
* Average basket value per customer

---

## ✔ Deliverables 

* OLTP SQL script
* DWH SQL script
* All Pentaho transformations (KTR/KJB)
* Dataset files (CSV)
* Power BI report (.pbix)
* This README.md

---


