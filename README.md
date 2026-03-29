# 🛒 E-Commerce Sales Analytics

An end-to-end data analytics pipeline that loads 550,000+ rows of real 
Brazilian e-commerce data into MySQL, runs advanced SQL analysis, and 
automatically generates professional Excel reports every week.

## 🏗️ Architecture
```
CSV Dataset → load_data.py → MySQL → analyze.py → report.py → Excel Report
(551,535 rows)  (ETL Load)          (8 SQL queries)  (6 sheets)
```

## ⚙️ Tech Stack
- **Language:** Python
- **Database:** MySQL
- **Libraries:** Pandas, SQLAlchemy, openpyxl, APScheduler, python-dotenv
- **Dataset:** Olist Brazilian E-Commerce (Kaggle)
- **Tools:** Git, VS Code

## 🚀 Features
- Loads 551,535+ rows across 7 related tables into MySQL
- Batch processing using chunksize for memory efficiency
- Advanced SQL queries — JOINs, CTEs, Window Functions, Subqueries
- Identifies top 10 revenue generating product categories
- Seasonal sales trend analysis (2016-2018)
- Regional revenue analysis by city and state
- Payment method breakdown and insights
- Automated weekly Excel report generation using APScheduler

## 📁 Project Structure
```
sales_analysis/
├── data/                    # Raw CSV files (not included in repo)
├── reports/                 # Generated Excel reports (auto-created)
├── load_data.py             # Loads CSV files into MySQL
├── analyze.py               # Runs 8 advanced SQL queries
├── report.py                # Generates Excel report with 6 sheets
├── pipeline.py              # Orchestrates full pipeline + scheduler
├── requirements.txt         # Project dependencies
└── README.md
```

## 📊 Dataset
| Table | Rows | Description |
|---|---|---|
| orders | 99,441 | Customer orders |
| order_items | 112,650 | Products in each order |
| customers | 99,441 | Customer information |
| products | 32,951 | Product details |
| sellers | 3,095 | Seller information |
| payments | 103,886 | Payment transactions |
| category_translation | 71 | Category name translations |
| **Total** | **551,535** | **Rows analyzed** |

## 🔍 SQL Queries & Insights

### 1. Top 10 Revenue Categories (JOIN + GROUP BY)
Health & Beauty leads with R$ 1,258,681 in revenue

### 2. Monthly Sales Trends (DATE functions)
November 2017 peak → R$ 1,153,528 (Black Friday effect!)
Business grew 10x from Oct 2016 to Aug 2018

### 3. Top Cities by Revenue (JOIN + GROUP BY)
São Paulo dominates → R$ 2,203,373 total revenue

### 4. Regional Analysis (JOIN + GROUP BY)
SP state → R$ 5,998,226 — 39% of total revenue

### 5. Payment Method Analysis (GROUP BY)
Credit Card → 74% of all transactions

### 6. High Value Orders (CTE)
4,296 orders worth more than R$500
Maximum single order → R$ 13,664.08

### 7. Running Total Revenue (Window Function)
Total revenue grew from R$46,566 → R$15,422,461

### 8. Above Average States (Subquery)
5 states perform above national average

## 📈 Excel Report — 6 Sheets
| Sheet | Content |
|---|---|
| 📊 Summary Dashboard | Key business metrics |
| 🏆 Top Categories | Revenue by category |
| 📅 Monthly Trends | Sales trends 2016-2018 |
| 🗺️ Regional Analysis | Cities and states |
| 💳 Payment Analysis | Payment methods |
| 🔍 Advanced SQL | CTE, Window, Subquery results |

## ▶️ How to Run
1. Clone the repo
2. Download dataset from Kaggle: [Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
3. Place CSV files in `data/` folder
4. Install dependencies: `pip install -r requirements.txt`
5. Create `.env` file:
```
DB_HOST=localhost
DB_PORT=3306
DB_NAME=sales_db
DB_USER=root
DB_PASSWORD=your_password
```
6. Create MySQL database: `CREATE DATABASE sales_db;`
7. Run full pipeline: `python pipeline.py`

## 📋 Requirements
```
pandas
sqlalchemy
mysql-connector-python
openpyxl
apscheduler
python-dotenv
```
```

---

## Now Create `requirements.txt`
```
pandas
sqlalchemy
mysql-connector-python
openpyxl
apscheduler
python-dotenv
```

---

## Now Create `.gitignore`
```
venv/
.env
__pycache__/
*.pyc
data/
reports/