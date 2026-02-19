# Lakehouse Demo - Azure Data Engineering

## 📋 Project Overview

This project demonstrates a **modern Data Lakehouse architecture** on Azure, 
built on the **WideWorldImporters (WWI)** sample dataset. Raw data flows through 
three medallion layers — **Bronze (raw)** → **Silver (cleaned)** → **Gold (aggregated)** — 
processed by Python-based ETL pipelines with automated testing and deployment via GitHub Actions.

---

## 🏗️ Architecture Overview

```
Bronze Layer          Silver Layer          Gold Layer
─────────────         ─────────────         ─────────────
Raw CSV files    →    Cleaned Parquet   →    Business-ready
(Azure Blob)          (typed, validated)     Parquet / Power BI
```

**Data Foundation:** Microsoft WideWorldImporters (WWI) – a realistic dataset 
for an international trading company covering sales, purchasing, and warehouse operations.

---

## 📁 Project Structure

```
lakehouse-demo/
├── data/
│   ├── bronze/          # Raw CSV files (source of truth)
│   ├── silver/          # Cleaned Parquet files
│   │   ├── sales/
│   │   ├── purchasing/
│   │   └── dimensions/
│   └── gold/            # Aggregated, analytics-ready data (coming soon)
├── src/
│   ├── etl/
│   │   ├── base_transformer.py       # Abstract base class for all transformers
│   │   ├── sales/                    # Sales transformers
│   │   ├── purchasing/               # Purchasing transformers
│   │   └── dimensions/               # Dimension transformers
│   ├── upload/                       # Azure Blob Storage upload
│   ├── config/
│   │   └── dimensions.yml            # Config-driven dimension transformer
│   ├── utils/
│   └── logs/
├── tests/                            # pytest test suite (coming soon)
├── run_all.py                        # Single entry point for full pipeline
├── pyproject.toml
├── environment.yml
└── .github/
    └── workflows/                    # GitHub Actions CI/CD (coming soon)
```

---

## 🥉 Bronze Layer

- Raw CSV files from WWI dataset, uploaded as-is to **Azure Blob Storage**
- No transformations applied – permanent source of truth
- 21 tables across Sales, Purchasing, and Application domains

---

## 🥈 Silver Layer

Cleaned and typed Parquet files, organized by domain:

| Domain | Tables |
|---|---|
| **Sales** | orders, order_lines, customers, invoices, invoice_lines |
| **Purchasing** | purchase_orders, purchase_order_lines, suppliers, supplier_transactions |
| **Dimensions** | cities, countries, provinces, people, delivery_methods, payment_methods, transaction_types, colors, package_types, stock_groups, stock_items, stock_item_holdings |

**Transformations applied:**
- PascalCase → snake_case column renaming
- String → `datetime64[ns]` casting for all date columns
- Nullable float IDs → `Int64` (pandas nullable integer)
- Empty columns dropped (documented per table)
- Null values validated and documented with business reasoning

---

## 🥇 Gold Layer *(in progress)*

Business-ready aggregations and Star Schema for analytics:

- `fact_orders` – order lines joined with orders and customers
- `fact_invoices` – invoice lines with revenue metrics
- `dim_customers` – enriched customer dimension
- `dim_products` – stock items with categories and suppliers
- Power BI reports on top of Gold layer

---

## ⚙️ ETL Design

The ETL is built on an **OOP inheritance pattern**:

```
BaseTransformer (abstract)
├── load_bronze()         # shared
├── _drop_empty_columns() # shared
├── _to_datetime()        # shared helper
├── save_silver()         # shared
├── run()                 # shared orchestration
└── transform()           # abstract → implemented per table

    ├── OrderTransformer
    ├── CustomerTransformer
    ├── DimensionTransformer  ← config-driven (dimensions.yml)
    └── ...
```

Run the full pipeline with a single command:
```bash
python run_all.py
```

---

## ☁️ Azure Integration

- **Azure Blob Storage** – Bronze and Silver layers stored in containers
- **Azure Data Factory** – Pipeline orchestration *(coming soon)*
- **GitHub Actions** – CI/CD for automated testing and deployment *(coming soon)*

---

## 🛠️ Setup

**1. Clone the repository**
```bash
git clone https://github.com/mibeit/lakehouse-demo.git
cd lakehouse-demo
```

**2. Create Conda environment**
```bash
conda env create -f environment.yml
conda activate lakehouse-demo
```

**3. Install as editable package**
```bash
pip install -e .
```

**4. Configure environment variables**
```bash
cp .env.example .env
# Add your Azure Storage connection string
```

**5. Run the full pipeline**
```bash
python run_all.py
```

---

## 📊 Dataset

**WideWorldImporters** is Microsoft's sample database for an international 
wholesale novelty goods importer. It covers:
- 40,000+ sales orders
- 127,000+ order lines
- 625 customers across multiple territories
- 227 stock items across 10 product groups
- Full purchasing and supplier transaction history

---

## 🗺️ Roadmap

- [x] Bronze Layer – raw CSV upload to Azure Blob Storage
- [x] Silver Layer – 21 tables cleaned and validated
- [ ] Gold Layer – Star Schema + business aggregations
- [ ] Azure Data Factory pipeline orchestration
- [ ] GitHub Actions CI/CD
- [ ] Power BI reports
- [ ] Monitoring and alerting
