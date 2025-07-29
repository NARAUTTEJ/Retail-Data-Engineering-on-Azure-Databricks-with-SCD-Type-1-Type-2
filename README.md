# 🛍️ Retail Data Engineering on Azure Databricks with SCD Type 1 & Type 2

**An end-to-end retail data pipeline using Azure Blob, Databricks, and Delta Live Tables with Medallion Architecture, implementing SCD1 & SCD2 for a star-schema model ready for BI and analytics.**

---

## 🔧 Project Overview  

This project demonstrates how to:  
- Ingest raw retail data (Products, Customers, Orders, Regions) from **Azure Blob Storage**  
- Build **Bronze**, **Silver**, and **Gold** layers using **Delta Lake**  
- Transform and cleanse data with **PySpark** and **Delta Live Tables (DLT)**  
- Apply **SCD Type 1** and **SCD Type 2** logic for dimension management  
- Model data into a **star schema** with fact and dimension tables  
- Enable downstream consumption via **Power BI, SQL Endpoints, and ML models**  

---

## 🏗️ Architecture  

![End-to-End Pipeline]![medallion_architecture](https://github.com/user-attachments/assets/1109849a-d94e-48e6-9353-3ccfb7600a4d)


### Layers  

- **Bronze**: Raw ingestion from `.parquet` files using Databricks  
- **Silver**: Cleaned and standardized data (deduplication, null handling, schema enforcement)  
- **Gold**: Curated, business-ready tables modeled in a star schema  

Additional Features:  
- **Unity Catalog** for secure data governance  
- **Delta Live Tables (DLT)** for reliable and scalable ETL pipelines  
- **Schema Evolution** to handle changing source structures  

---

## ⭐ Star Schema  

![Fact & Dimension Model]![Data_flow](https://github.com/user-attachments/assets/92a784c1-d248-4a90-969b-54a6860cf5c4)
 

### Gold Layer Tables  

- `gold.dim_customers` – **SCD Type 1** (latest attribute values)  
- `gold.dim_products` – **SCD Type 2** (historical tracking using DLT `apply_changes`)  
- `gold.fact_orders` – Transaction facts linked to dimension tables  

---

## 🔁 Notebooks Overview  

| Layer   | Notebook / Pipeline          | Description                               |
|---------|------------------------------|-------------------------------------------|
| Gold    | `gold_dim_customers.py`      | Creates Dim_Customers with **SCD1** logic |
| Gold    | `gold_dim_products.py`       | Creates Dim_Products with **SCD2** via DLT|
| Gold    | `gold_fact_orders.py`        | Builds Fact Orders joining all dimensions |
| Silver  | `silver_customers.py`        | Cleansing and transforming customers      |
| Silver  | `silver_orders.py`           | Processing raw orders data                |
| Silver  | `silver_regions.py`          | Transforming regions metadata             |
| Bronze  | `bronze_ingestion.py`        | Raw ingestion from Azure Blob Storage     |

---

## ⚙️ Technologies Used  

- **Azure Blob Storage**  
- **Databricks**  
- **Unity Catalog**  
- **PySpark**  
- **Delta Lake**  
- **Delta Live Tables (DLT)**  
- **Power BI / SQL Endpoints**  

---

## ✅ Features  

- End-to-end **Medallion Architecture** (Bronze → Silver → Gold)  
- **SCD1 & SCD2** for dimension management  
- **Schema Evolution & Governance** via Unity Catalog  
- **Automated ETL** using Delta Live Tables  
- **Star Schema** for optimized BI & analytics  
- Ready for **Power BI dashboards**, **SQL queries**, and **ML models**  

---

## 📊 Consumption Layer  

Data from the Gold layer is consumed via:  
- 🔶 **Power BI dashboards**  
- 🔍 **Ad-hoc SQL queries** through Databricks SQL endpoints  
- 🤖 **Machine Learning models** (optional extension)  

---

## 📂 Repository Structure  

```plaintext
azure-retail-pipeline/
├── 1_Bronze/
│   └── bronze_ingestion.py
├── 2_Silver/
│   ├── silver_customers.py
│   ├── silver_orders.py
│   └── silver_regions.py
├── 3_Gold/
│   ├── gold_dim_customers.py
│   ├── gold_dim_products.py
│   └── gold_fact_orders.py
├── images/
│   ├── END_TO_END_PIPELINE.png
│   ├── DLT_PIPELINE.png
│   └── FACT_TABLE.png
└── README.md

# 📝 Author
Nara Uttej
Data Engineering Enthusiast | Azure | Databricks | PySpark | SQL
