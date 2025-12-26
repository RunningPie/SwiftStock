-- 1. ADD COLUMNS FOR PREDICTION
-- We need a place to store our "AI Forecast"
ALTER TABLE INVENTORY_DAILY ADD COLUMN IF NOT EXISTS AVG_DAILY_USAGE FLOAT;
ALTER TABLE INVENTORY_DAILY ADD COLUMN IF NOT EXISTS PREDICTED_STOCKOUT_DAYS INT;

-- 2. CREATE A STREAM (Requirement: Streams)
-- This watches the inventory table for changes (e.g., new uploads)
CREATE OR REPLACE STREAM INVENTORY_STREAM ON TABLE INVENTORY_DAILY;

-- 3. CREATE SNOWPARK STORED PROCEDURE (Requirement: Snowpark for Forecasting)
-- This simulates a Machine Learning model calculating demand
CREATE OR REPLACE PROCEDURE PREDICT_DEMAND()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import col, floor

def main(session):
    # 1. Read the Inventory Table
    df = session.table("INVENTORY_DAILY")
    
    # 2. "The Forecast Logic" 
    # In a real hackathon, simple logic is safer than complex ML.
    # We assume Usage is roughly 10% of Opening Stock + random noise simulation
    # Logic: Avg Usage = Opening_Stock * 0.15 (Simulating high consumption)
    df_predicted = df.with_column("AVG_DAILY_USAGE", col("OPENING_STOCK") * 0.15)
    
    # 3. Calculate Days Until Stockout
    # Logic: Current Stock / Avg Usage
    df_final = df_predicted.with_column("PREDICTED_STOCKOUT_DAYS", 
                                        floor(col("CLOSING_STOCK") / col("AVG_DAILY_USAGE")))

    # 4. Write back to table (Overwrite for MVP simplicity)
    df_final.write.mode("overwrite").save_as_table("INVENTORY_DAILY_PREDICTED")
    
    return "Demand Forecast Updated Successfully"
$$;

-- 4. CREATE A TASK (Requirement: Tasks)
-- Triggers the Snowpark prediction every 5 minutes IF data changed
CREATE OR REPLACE TASK UPDATE_FORECAST_TASK
    WAREHOUSE = SWIFTSTOCK_WH
    SCHEDULE = '5 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('INVENTORY_STREAM')
AS
    CALL PREDICT_DEMAND();

-- Manually Run it once to populate data (since Stream is empty right now)
CALL PREDICT_DEMAND();
ALTER TASK UPDATE_FORECAST_TASK RESUME;

-- 5. CREATE DYNAMIC TABLE (Requirement: Dynamic Tables)
-- This creates the "Live Reorder List" for the procurement team
CREATE OR REPLACE DYNAMIC TABLE REORDER_ALERTS
    TARGET_LAG = '1 MINUTE'
    WAREHOUSE = SWIFTSTOCK_WH
AS
    SELECT 
        FACILITY_ID,
        ITEM_NAME,
        CLOSING_STOCK,
        AVG_DAILY_USAGE,
        PREDICTED_STOCKOUT_DAYS,
        -- Logic: If we have < 7 days stock, order enough for 30 days
        CASE 
            WHEN PREDICTED_STOCKOUT_DAYS < 7 THEN 'URGENT REORDER'
            WHEN PREDICTED_STOCKOUT_DAYS < 14 THEN 'WARNING'
            ELSE 'OK'
        END AS STATUS,
        CASE 
            WHEN PREDICTED_STOCKOUT_DAYS < 14 THEN (30 * AVG_DAILY_USAGE) - CLOSING_STOCK
            ELSE 0 
        END AS SUGGESTED_REORDER_QTY
    FROM INVENTORY_DAILY_PREDICTED
    WHERE STATUS != 'OK';