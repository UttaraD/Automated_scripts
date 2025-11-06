"""
python_snowflake_sql_combo.py

Requires:
  pip install snowflake-connector-python pandas

Set env vars or inline the creds below:
  SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
"""

import os
import pandas as pd
import snowflake.connector as sf


# ---- 1) Connection ----
conn = sf.connect(
    user=os.getenv("SF_USER", "<YOUR_USER>"),
    password=os.getenv("SF_PASSWORD", "<YOUR_PASSWORD>"),
    account=os.getenv("SF_ACCOUNT", "<YOUR_ACCOUNT>"),      # e.g., "xy12345.us-east-1"
    warehouse=os.getenv("SF_WAREHOUSE", "COMPUTE_WH"),
    database=os.getenv("SF_DATABASE", "DEMO_DB"),
    schema=os.getenv("SF_SCHEMA", "PUBLIC"),
    role=os.getenv("SF_ROLE", "ACCOUNTADMIN"),
)


# ---- 2) SQL blocks (permanent sources + temp pipeline + final table) ----
sql_setup_sources = """
-- Workspace
USE DATABASE DEMO_DB;
USE SCHEMA PUBLIC;

-- PERMANENT source tables
CREATE OR REPLACE TABLE CUSTOMERS (
  CUSTOMER_ID INT,
  CUSTOMER_NAME STRING,
  STATE STRING
);
INSERT OVERWRITE INTO CUSTOMERS VALUES
  (1,'Alice','CA'),
  (2,'Bob','NY'),
  (3,'Chloe','TX'),
  (4,'Dev','WA');

CREATE OR REPLACE TABLE PRODUCTS (
  PRODUCT_ID INT,
  PRODUCT_NAME STRING,
  CATEGORY STRING,
  PRICE NUMBER(10,2)
);
INSERT OVERWRITE INTO PRODUCTS VALUES
  (10,'Keyboard','Accessories',29.99),
  (11,'Mouse','Accessories',19.99),
  (12,'Monitor','Hardware',199.00),
  (13,'Laptop','Hardware',899.00);

CREATE OR REPLACE TABLE ORDERS (
  ORDER_ID INT,
  CUSTOMER_ID INT,
  ORDER_DATE DATE
);
INSERT OVERWRITE INTO ORDERS VALUES
  (1001,1,'2025-10-20'),
  (1002,1,'2025-11-01'),
  (1003,2,'2025-10-28'),
  (1004,3,'2025-11-03'),
  (1005,4,'2025-11-05');

CREATE OR REPLACE TABLE ORDER_ITEMS (
  ORDER_ID INT,
  PRODUCT_ID INT,
  QTY INT
);
INSERT OVERWRITE INTO ORDER_ITEMS VALUES
  (1001,10,1),
  (1001,11,2),
  (1002,12,1),
  (1003,10,1),
  (1003,11,1),
  (1004,12,2),
  (1005,13,1);
"""

sql_pipeline = """
-- TEMP staging: recent orders (last 30 days)
CREATE OR REPLACE TEMP TABLE TMP_RECENT_ORDERS AS
SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE
FROM ORDERS
WHERE ORDER_DATE >= CURRENT_DATE() - 30;

-- TEMP staging: per-order totals (join + aggregate)
CREATE OR REPLACE TEMP TABLE TMP_ORDER_TOTALS AS
SELECT
  oi.ORDER_ID,
  SUM(oi.QTY * p.PRICE) AS ORDER_TOTAL
FROM ORDER_ITEMS oi
JOIN PRODUCTS p ON p.PRODUCT_ID = oi.PRODUCT_ID
GROUP BY oi.ORDER_ID;

-- TEMP staging: latest recent order per customer (window + QUALIFY)
CREATE OR REPLACE TEMP TABLE TMP_LATEST_ORDER_PER_CUSTOMER AS
SELECT
  ro.CUSTOMER_ID,
  ro.ORDER_ID,
  ro.ORDER_DATE,
  ROW_NUMBER() OVER (PARTITION BY ro.CUSTOMER_ID ORDER BY ro.ORDER_DATE DESC) AS rn
FROM TMP_RECENT_ORDERS ro
QUALIFY rn = 1;

-- PERMANENT final table combining permanent + temp
CREATE OR REPLACE TABLE FINAL_CUSTOMER_REPORT AS
SELECT
  c.CUSTOMER_ID,
  c.CUSTOMER_NAME,
  c.STATE,
  lo.ORDER_ID AS LATEST_ORDER_ID,
  lo.ORDER_DATE AS LATEST_ORDER_DATE,
  ot.ORDER_TOTAL AS LATEST_ORDER_TOTAL
FROM CUSTOMERS c
LEFT JOIN TMP_LATEST_ORDER_PER_CUSTOMER lo
  ON lo.CUSTOMER_ID = c.CUSTOMER_ID
LEFT JOIN TMP_ORDER_TOTALS ot
  ON ot.ORDER_ID = lo.ORDER_ID;
"""

sql_preview = """
-- Retu
