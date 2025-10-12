# Apple Retail Sales Analytics on Azure

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?logo=apache-spark&style=flat-square)
![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoft-azure&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue?logo=microsoft-azure&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=databricks&style=flat-square)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?logo=power-bi&style=flat-square)
![Git](https://img.shields.io/badge/Git-CI%2FCD-green?logo=git&style=flat-square)

---

## 📑 Table of Contents
- [📌 Project Overview](#-project-overview)
  - [1. End-to-End Flow](#-end-to-end-flow)
  - [2. Key Highlights](#-key-highlights)
- [🎯 Objectives](#-objectives)
- [📂 Project Structure](#-project-structure)
- [🛠️ Tools & Technologies](#️-tools--technologies)
- [📐 Data Architecture](#-data-architecture)
- [⭐ Star Schema Design](#-star-schema-design)
- [⚙️ Step-by-Step Implementation](#️-step-by-step-implementation)
  - [1. Data Ingestion](#1-data-ingestion-azure-data-factory-)
  - [2. Data Transformation](#2-data-transformation-azure-databricks-)
  - [3. Data Warehouse](#3-data-warehouse-azure-synapse-analytics-)
  - [4. Version Control (GitHub)](#4-version-control-github-)
- [📊 Data Analytics](#-data-analytics-)
  - [Synapse → Power BI Connection](#-synapse--power-bi-connection-)
  - [Dashboard Insights](#-dashboard-insights-)
  - [KPI Reports](#-kpi-reports-)
- [✅ Key Outcomes](#-key-outcomes)
- [👨‍💻 Author](#-author-)
---

## 📌 Project Overview

This project demonstrates an **end-to-end data engineering and analytics pipeline** for **Apple Retail Stores** using the **Microsoft Azure ecosystem**. The workflow begins with **Azure Data Factory (ADF)** ingesting raw retail data from multiple sources (mainly from GitHub) into **Azure Data Lake Storage**. The data is then transformed and enriched in **Azure Databricks (PySpark)** through a **Bronze–Silver–Gold architecture**, ensuring data quality, consistency, and scalability.  
The curated Gold Layer data is loaded into **Azure Synapse Analytics**, structured in a **Star Schema** format optimized for analytical queries. Finally, the data is connected to **Power BI** to create interactive dashboards that visualize key business insights such as sales performance, product profitability, and store-level metrics across regions.

### 🔁 End-to-End Flow

**ADF (Ingestion)** ➜ **Databricks (Transformation)** ➜ **Synapse (Data Warehouse)** ➜ **Power BI (Visualization)** <br />
<img alt="flowchart" src="/flowchart.png"/>

---

### 🧠 Key Highlights
- **Automated ingestion** using ADF pipelines for raw sales, product, and store data.  
- **Data transformation** using PySpark notebooks in Databricks following the **Bronze–Silver–Gold** model.  
- **Centralized data warehouse** in Synapse Analytics for efficient querying.  
- **KPI dashboards** in Power BI showcasing business insights and performance trends.  

---

## 🎯 Objectives
- Ingest raw data from GitHub via **Azure Data Factory (ADF)**
- Process retail sales data to analyze **product, category, and store performance**.  
- Design a **bronze–silver–gold layered architecture** in **Azure Data Lake**.  
- Build a **star schema** optimized for analytical queries.  
- Create **business KPIs** using **Synapse SQL** views and **Power BI**.  
- Implement **reproducible and scalable** data engineering practices.

---

## 📂 Project Structure
```plaintext
apple-retail-sales-analysis-data-engineering/
│
├── databricks-notebooks/
│   ├── bronze_layer.ipynb
│   ├── silver_layer.ipynb
│   └── gold_layer.ipynb
│
│── kpi/
│   ├── reports/
│       ├── avg_price_by_category.pdf
│       ├── top_10_best_selling_products.pdf
│       ├── total_sales_by_category.pdf
│       ├── total_sales_by_country.pdf
│       ├── total_sales_revenue.pdf
│       └── total_yearly_revenue.pdf
│   ├── raw_pbix_kpi_files/
│       ├── avg_price_by_category.pbix
│       ├── top_10_best_selling_products.pbix
│       ├── total_sales_by_category.pbix
│       ├── total_sales_by_country.pbix
│       ├── total_sales_revenue.pbix
│       └── total_yearly_revenue.pbix
│   ├── KPI_Summary.md
│
├── raw-data-source/
│   ├── category.csv
│   ├── products.csv
│   ├── sales.csv
│   ├── stores.csv
│   └── warranty.csv
│
├── sql-queries/
│   ├── ddl_commands.sql
│   ├── kpi_insight_query_cmds.sql
│
├── README.md
├── flowchart.png
├── dashboard.png
└── .gitignore
```
---

## 🛠️ Tools & Technologies  

- **Azure Data Factory (ADF)** – Orchestrates data ingestion and pipeline scheduling  
- **Azure Databricks** – PySpark-based ETL and transformation workflows  
- **Azure Data Lake Storage (ADLS)** – Stores raw (Bronze), cleaned (Silver), and curated (Gold) datasets  
- **Azure Synapse Analytics (SQL Pool)** – Serves as the enterprise data warehouse for analytics  
- **Power BI** – Business intelligence dashboarding and KPI visualization.  
- **Python 3.9+** – Core programming for ETL logic and transformation scripts  
- **Git** – Version control and collaboration  

---

## 📐 Data Architecture  

The pipeline follows a **multi-layered architecture** to ensure scalability, maintainability, and data quality:  

### 🟤 Bronze Layer  
- Stores **raw CSV data** from retail sources in **Azure Data Lake (ADLS Gen2)**.  
- Acts as the immutable source of truth for all further transformations.  

### ⚪ Silver Layer  
- Performs **data cleaning, validation, and standardization** in Azure Databricks.  
- Handles schema corrections, null removal, and type casting.  

### 🟡 Gold Layer  
- Contains **aggregated and transformed data** optimized for analytics and BI.  
- Stored in **Azure Synapse SQL Pool** following a **Star Schema** model.  

---

## ⭐ Star Schema Design  

The **Gold Layer** in **Azure Synapse** is structured for efficient analytical querying and KPI generation.  

**Fact Table:**  
- `FactSales` – Contains sales transactions, revenue, quantity, warranty claims, and product/store references (Surrogate Keys).  

**Dimension Tables:**  
- `DimProduct` – Product details (product name, price, launch date).  
- `DimCategory` – Category details of various products.  
- `DimStore` – Store location, country, and region details.

---

## ⚙️ Step-by-Step Implementation  

### 1. **Data Ingestion (Azure Data Factory)**  
- Configured **ADF pipelines** to import CSV product, sales, stores, warranty, and category data from Github source into **Azure Data Lake (Bronze Layer)**.  
- Scheduled pipelines for periodic refresh.  

### 2. **Data Transformation (Azure Databricks)**  
- Connected ADF to Databricks for automated job triggers.  
- Created PySpark notebooks to:  
  - Read raw data from the Bronze layer.  
  - Clean and validate schema like handled mismatched data types like `launch_date` as well as handle any null values that were present within the tables.  
  - Generate Fact and Dimension tables.  
  - Write curated Delta tables to the **Gold Layer** in ADLS.  

### 3. **Data Warehouse (Azure Synapse Analytics)**  
- Created **external tables** in Synapse mapped to Delta files in ADLS Gold.  
- Defined **views for KPIs** such as total revenue, top products, and country-wise sales.  
- Enabled **Power BI connectivity** using the Synapse SQL endpoint.  

### 4. **Version Control (GitHub)**  
- Managed notebooks, SQL scripts, and transformation code in a **Git repository**.  
- Used separate branches for development and production.  

---

## 📊 Data Analytics  

Once the Gold Layer tables were ready, **Azure Synapse SQL Pool** served as the source for analytical queries and Power BI dashboards.  

### 🔗 Synapse → Power BI Connection  
- Established a **Direct SQL Connection** between Synapse and Power BI Service (as Power BI Desktop is unavailable on macOS).  
- Imported Fact and Dimension tables into Power BI datasets.  
- Created relationships to preserve the **Star Schema model**.  

### 📈 Dashboard Insights  
The **Apple Retail Sales Dashboard** provides and facilitates KPIs such as:  
- 💰 **Total Sales Revenue**  
- 🏆 **Top 10 Best-Selling Products**
- 🌍 **Total Sales by Country**  
- 📊 **Average Price by Category**  
- 📅 **Annual Quarterly Revenues**  
- 📋 **Total Sales by Category**

<br />
<img alt="flowchart" src="/dashboard.png"/>

### 🧾 KPI Reports  
Exported analytical summaries as PDF reports (for reference):  
- `kpi/reports/avg_price_by_category.pdf`  
- `kpi/reports/top_10_best_selling_products.pdf`  
- `kpi/reports/total_sales_by_category.pdf`  
- `kpi/reports/total_sales_by_country.pdf`  
- `kpi/reports/total_sales_revenue.pdf`  
- `kpi/reports/total_yearly_revenue.pdf`  

---

## ✅ Key Outcomes  

- **End-to-End Azure Pipeline:** From ingestion → transformation → warehousing → analytics.  
- **Modular Architecture:** Clear separation between Bronze, Silver, and Gold data layers.  
- **Business Insights:** Identified top-performing products, profitable categories, and regional sales trends.  
- **Portfolio Value:** Demonstrates expertise across **ADF, Databricks, Synapse, and Power BI**.  



---

### 👨‍💻 Author  
**Pratyush Sinha**  
📧 Email: **pratisinha@gmail.com**  
🔗 LinkedIn: [linkedin.com/pratyushsinha](https://linkedin.com/in/pratyushsinha213)  