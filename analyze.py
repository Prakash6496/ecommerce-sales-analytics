import mysql.connector
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

def get_connection():
    password = quote_plus(os.getenv("DB_PASSWORD"))
    url = f"mysql+mysqlconnector://root:{password}@localhost:3306/sales_db"
    return create_engine(url)


def run_query(query, description):
    engine = get_connection()
    df = pd.read_sql(query, engine)
    engine.dispose()
    print(f"\n{'='*50}")
    print(f"📊 {description}")
    print(f"{'='*50}")
    print(df.to_string(index=False))
    return df

# ─────────────────────────────────────────
# QUERY 1 — Top 10 Revenue Generating Categories
# Uses: JOIN, GROUP BY, ORDER BY
# ─────────────────────────────────────────
query1 = """
SELECT 
    ct.product_category_name_english AS category,
    COUNT(oi.order_id)               AS total_orders,
    ROUND(SUM(oi.price), 2)          AS total_revenue,
    ROUND(AVG(oi.price), 2)          AS avg_price
FROM order_items oi
JOIN products p  
    ON oi.product_id = p.product_id
JOIN category_translation ct 
    ON p.product_category_name = ct.product_category_name
GROUP BY ct.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 10;
"""

# ─────────────────────────────────────────
# QUERY 2 — Monthly Sales Trends (Seasonal)
# Uses: DATE functions, GROUP BY, ORDER BY
# ─────────────────────────────────────────
query2 = """
SELECT
    YEAR(o.order_purchase_timestamp)  AS year,
    MONTH(o.order_purchase_timestamp) AS month,
    COUNT(o.order_id)                 AS total_orders,
    ROUND(SUM(p.payment_value), 2)    AS total_revenue
FROM orders o
JOIN payments p
    ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY year, month
ORDER BY year, month;
"""

# ─────────────────────────────────────────
# QUERY 3 — Top 10 Customers by Revenue
# Uses: JOIN, GROUP BY, ORDER BY, LIMIT
# ─────────────────────────────────────────
query3 = """
SELECT
    c.customer_city,
    c.customer_state,
    COUNT(o.order_id)            AS total_orders,
    ROUND(SUM(p.payment_value), 2) AS total_spent
FROM customers c
JOIN orders o   
    ON c.customer_id = o.customer_id
JOIN payments p 
    ON o.order_id = p.order_id
GROUP BY c.customer_city, c.customer_state
ORDER BY total_spent DESC
LIMIT 10;
"""

# ─────────────────────────────────────────
# QUERY 4 — Revenue by Region (State)
# Uses: JOIN, GROUP BY, ORDER BY
# ─────────────────────────────────────────
query4 = """
SELECT
    c.customer_state            AS state,
    COUNT(o.order_id)           AS total_orders,
    ROUND(SUM(p.payment_value), 2) AS total_revenue
FROM customers c
JOIN orders o   
    ON c.customer_id = o.customer_id
JOIN payments p 
    ON o.order_id = p.order_id
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 10;
"""

# ─────────────────────────────────────────
# QUERY 5 — Payment Method Analysis
# Uses: GROUP BY, COUNT, percentage calculation
# ─────────────────────────────────────────
query5 = """
SELECT
    payment_type,
    COUNT(*)                     AS total_transactions,
    ROUND(SUM(payment_value), 2) AS total_value,
    ROUND(AVG(payment_value), 2) AS avg_value
FROM payments
GROUP BY payment_type
ORDER BY total_transactions DESC;
"""

# ─────────────────────────────────────────
# QUERY 6 — CTE: High Value Orders
# Uses: CTE (WITH clause)
# ─────────────────────────────────────────
query6 = """
WITH order_totals AS (
    SELECT
        order_id,
        ROUND(SUM(payment_value), 2) AS total_value
    FROM payments
    GROUP BY order_id
),
high_value AS (
    SELECT *
    FROM order_totals
    WHERE total_value > 500
)
SELECT
    COUNT(*)             AS high_value_orders,
    ROUND(AVG(total_value), 2) AS avg_order_value,
    ROUND(MAX(total_value), 2) AS max_order_value
FROM high_value;
"""

# ─────────────────────────────────────────
# QUERY 7 — Window Function: Running Total Revenue
# Uses: Window Function (SUM OVER)
# ─────────────────────────────────────────
query7 = """
SELECT
    YEAR(o.order_purchase_timestamp)  AS year,
    MONTH(o.order_purchase_timestamp) AS month,
    ROUND(SUM(p.payment_value), 2)    AS monthly_revenue,
    ROUND(SUM(SUM(p.payment_value)) 
        OVER (ORDER BY YEAR(o.order_purchase_timestamp), 
                       MONTH(o.order_purchase_timestamp)
        ), 2)                         AS running_total
FROM orders o
JOIN payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY year, month
ORDER BY year, month;
"""

# ─────────────────────────────────────────
# QUERY 8 — Subquery: Above Average Orders
# Uses: Subquery
# ─────────────────────────────────────────
query8 = """
SELECT
    c.customer_state,
    COUNT(o.order_id) AS total_orders
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_state
HAVING COUNT(o.order_id) > (
    SELECT AVG(state_orders)
    FROM (
        SELECT COUNT(o2.order_id) AS state_orders
        FROM customers c2
        JOIN orders o2 ON c2.customer_id = o2.customer_id
        GROUP BY c2.customer_state
    ) AS subquery
)
ORDER BY total_orders DESC;
"""

def run_analysis():
    print("\n🔍 Starting Sales Analysis...")

    results = {}
    results["top_categories"]   = run_query(query1, "Top 10 Revenue Generating Categories")
    results["monthly_trends"]   = run_query(query2, "Monthly Sales Trends")
    results["top_cities"]       = run_query(query3, "Top 10 Cities by Revenue")
    results["regional_revenue"] = run_query(query4, "Top 10 States by Revenue")
    results["payment_methods"]  = run_query(query5, "Payment Method Analysis")
    results["high_value_orders"]= run_query(query6, "High Value Orders (CTE)")
    results["running_total"]    = run_query(query7, "Running Total Revenue (Window Function)")
    results["above_avg_states"] = run_query(query8, "Above Average States (Subquery)")

    print("\n🎉 Analysis Complete!")
    return results

if __name__ == "__main__":
    run_analysis()