# pip install snowflake-connector-python pandas
import os
import pandas as pd
import snowflake.connector as sf

# --- Fill in your connection details (env vars or inline) ---
conn = sf.connect(
    user=os.getenv("SF_USER", "<YOUR_USER>"),
    password=os.getenv("SF_PASSWORD", "<YOUR_PASSWORD>"),
    account=os.getenv("SF_ACCOUNT", "<YOUR_ACCOUNT>"),  # e.g., "xy12345.us-east-1"
    warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    database=os.getenv("SF_DATABASE", "DEMO_DB"),
    schema=os.getenv("SF_SCHEMA", "PUBLIC"),
    role=os.getenv("SF_ROLE", "ACCOUNTADMIN")
)

sql_script = """
USE DATABASE DEMO_DB;
USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE CUSTOMERS AS
SELECT * FROM VALUES
    (1, 'Alice', 'CA'),
    (2, 'Bob',   'NY'),
    (3, 'Chloe', 'TX')
AS v(CUSTOMER_ID, CUSTOMER_NAME, STATE);

CREATE OR REPLACE TABLE PRODUCTS AS
SELECT * FROM VALUES
    (10, 'Keyboard',  29.99),
    (11, 'Mouse',     19.99),
    (12, 'Monitor',  199.00)
AS v(PRODUCT_ID, PRODUCT_NAME, PRICE);

CREATE OR REPLACE TABLE ORDERS AS
SELECT * FROM VALUES
    (1001, 1, '2025-10-20'::DATE),
    (1002, 1, '2025-11-01'::DATE),
    (1003, 2, '2025-10-28'::DATE),
    (1004, 3, '2025-11-03'::DATE)
AS v(ORDER_ID, CUSTOMER_ID, ORDER_DATE);

CREATE OR REPLACE TABLE ORDER_ITEMS AS
SELECT * FROM VALUES
    (1001, 10, 1),
    (1001, 11, 2),
    (1002, 12, 1),
    (1003, 10, 1),
    (1003, 11, 1),
    (1004, 12, 2)
AS v(ORDER_ID, PRODUCT_ID, QTY);

CREATE OR REPLACE TEMP TABLE TMP_RECENT_ORDERS AS
SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    o.ORDER_DATE
FROM ORDERS o
WHERE o.ORDER_DATE >= CURRENT_DATE() - 30;

CREATE OR REPLACE TEMP TABLE TMP_ORDER_TOTALS AS
SELECT
    oi.ORDER_ID,
    SUM(oi.QTY * p.PRICE) AS ORDER_TOTAL
FROM ORDER_ITEMS oi
JOIN PRODUCTS p
  ON p.PRODUCT_ID = oi.PRODUCT_ID
GROUP BY oi.ORDER_ID;

CREATE OR REPLACE TEMP TABLE TMP_LATEST_ORDER_PER_CUSTOMER AS
SELECT
    o.CUSTOMER_ID,
    o.ORDER_ID,
    o.ORDER_DATE,
    ROW_NUMBER() OVER (PARTITION BY o.CUSTOMER_ID ORDER BY o.ORDER_DATE DESC) AS rn
FROM TMP_RECENT_ORDERS o
QUALIFY rn = 1;

CREATE OR REPLACE TEMP TABLE TMP_CUSTOMER_LATEST_ORDER AS
SELECT
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.STATE,
    lo.ORDER_ID,
    lo.ORDER_DATE,
    ot.ORDER_TOTAL
FROM CUSTOMERS c
LEFT JOIN TMP_LATEST_ORDER_PER_CUSTOMER lo
  ON lo.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN TMP_ORDER_TOTALS ot
  ON ot.ORDER_ID = lo.ORDER_ID;

-- Final result
SELECT *
FROM TMP_CUSTOMER_LATEST_ORDER
ORDER BY CUSTOMER_ID;
"""

def run_multi(conn, sql_block):
    # The Snowflake connector executes one statement at a time,
    # so we split on semicolons safely (ignoring blank lines).
    statements = [s.strip() for s in sql_block.split(';') if s.strip()]
    df_final = None
    with conn.cursor() as cur:
        for i, stmt in enumerate(statements, start=1):
            cur.execute(stmt)
            # The very last statement returns the final result set
            if i == len(statements):
                try:
                    df_final = cur.fetch_pandas_all()
                except Exception:
                    # last statement didn’t return rows (unlikely here)
                    df_final = None
    return df_final

df = run_multi(conn, sql_script)

print("Result from TMP_CUSTOMER_LATEST_ORDER:")
print(df if df is not None else "No rows returned")

conn.close()
