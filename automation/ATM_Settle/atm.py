# pip install snowflake-connector-python pandas
import os
import pandas as pd
import snowflake.connector as sf

conn = sf.connect(
    user=os.getenv("SF_USER", "<USER>"),
    password=os.getenv("SF_PASSWORD", "<PASSWORD>"),
    account=os.getenv("SF_ACCOUNT", "<ACCOUNT>"),         # e.g. "xy12345.us-east-1"
    warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    database=os.getenv("SF_DATABASE", "DEMO_DB"),
    schema=os.getenv("SF_SCHEMA", "PUBLIC"),
    role=os.getenv("SF_ROLE", "ACCOUNTADMIN"),
)

sql_block = """
USE DATABASE DEMO_DB; USE SCHEMA PUBLIC;

CREATE OR REPLACE TABLE CUSTOMERS (CUSTOMER_ID INT, CUSTOMER_NAME STRING, STATE STRING);
CREATE OR REPLACE TABLE PRODUCTS (PRODUCT_ID INT, PRODUCT_NAME STRING, PRICE NUMBER(10,2));
CREATE OR REPLACE TABLE ORDERS (ORDER_ID INT, CUSTOMER_ID INT, ORDER_DATE DATE);
CREATE OR REPLACE TABLE ORDER_ITEMS (ORDER_ID INT, PRODUCT_ID INT, QTY INT);

INSERT OVERWRITE INTO CUSTOMERS VALUES
  (1,'Alice','CA'),(2,'Bob','NY'),(3,'Chloe','TX');
INSERT OVERWRITE INTO PRODUCTS VALUES
  (10,'Keyboard',29.99),(11,'Mouse',19.99),(12,'Monitor',199.00);
INSERT OVERWRITE INTO ORDERS VALUES
  (1001,1,'2025-10-20'),(1002,1,'2025-11-01'),
  (1003,2,'2025-10-28'),(1004,3,'2025-11-03');
INSERT OVERWRITE INTO ORDER_ITEMS VALUES
  (1001,10,1),(1001,11,2),(1002,12,1),
  (1003,10,1),(1003,11,1),(1004,12,2);

CREATE OR REPLACE TEMP TABLE TMP_RECENT_ORDERS AS
SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE
FROM ORDERS
WHERE ORDER_DATE >= CURRENT_DATE() - 30;

CREATE OR REPLACE TEMP TABLE TMP_ORDER_TOTALS AS
SELECT oi.ORDER_ID, SUM(oi.QTY * p.PRICE) AS ORDER_TOTAL
FROM ORDER_ITEMS oi
JOIN PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
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
  c.CUSTOMER_ID, c.CUSTOMER_NAME, c.STATE,
  lo.ORDER_ID, lo.ORDER_DATE, ot.ORDER_TOTAL
FROM CUSTOMERS c
LEFT JOIN TMP_LATEST_ORDER_PER_CUSTOMER lo
  ON lo.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN TMP_ORDER_TOTALS ot
  ON ot.ORDER_ID = lo.ORDER_ID;

SELECT * FROM TMP_CUSTOMER_LATEST_ORDER ORDER BY CUSTOMER_ID;
"""

def run_multistatement(conn, sql):
    results = None
    with conn.cursor() as cur:
        for i, stmt in enumerate([s.strip() for s in sql.split(';') if s.strip()], start=1):
            cur.execute(stmt)
            if i == len([s for s in sql.split(';') if s.strip()]):
                try:
                    results = cur.fetch_pandas_all()
                except Exception:
                    results = None
    return results

df = run_multistatement(conn, sql_block)
print(df)
conn.close()
