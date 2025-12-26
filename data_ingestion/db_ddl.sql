CREATE OR REPLACE TABLE FACILITIES (
    FACILITY_ID VARCHAR(10) PRIMARY KEY, -- e.g., 'WJ-001'
    FACILITY_NAME VARCHAR(100),          -- e.g., 'RS Hasan Sadikin'
    CITY VARCHAR(50),                    -- e.g., 'Bandung'
    LATITUDE FLOAT,                      -- e.g., -6.8943
    LONGITUDE FLOAT,                     -- e.g., 107.5973
    GEO_POINT GEOGRAPHY                  -- Auto-generated from Lat/Long
);

CREATE OR REPLACE TABLE INVENTORY_DAILY (
    RECORD_ID VARCHAR(36) DEFAULT UUID_STRING(), -- Unique ID for the row
    DATE DATE,                                   -- The date of the snapshot
    FACILITY_ID VARCHAR(10),                     -- FK linking to FACILITIES
    ITEM_NAME VARCHAR(50),                       -- e.g., 'Paracetamol 500mg'
    CATEGORY VARCHAR(30),                        -- e.g., 'Antibiotics', 'PPE'
    
    -- Required by Problem Statement:
    OPENING_STOCK INT,
    RECEIVED_QTY INT,
    ISSUED_QTY INT,
    CLOSING_STOCK INT,      -- Logic: Opening + Received - Issued
    LEAD_TIME_DAYS INT,     -- How long it takes to order from supplier
    
    -- For your USP:
    CRITICALITY_LEVEL VARCHAR(10) -- 'High', 'Medium', 'Low' (Helps the AI prioritize)
);

-- 5. Create Transaction Logs (Empty for now, populated by App later)
CREATE OR REPLACE TABLE TRANSFER_LOGS (
    TRANSFER_ID VARCHAR(36) DEFAULT UUID_STRING(),
    DATE_LOGGED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FACILITY_ID VARCHAR(10),
    DEST_FACILITY_ID VARCHAR(10),
    ITEM_NAME VARCHAR(50),
    QTY_TRANSFERRED INT,
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    CONSTRAINT fk_source FOREIGN KEY (SOURCE_FACILITY_ID) REFERENCES FACILITIES(FACILITY_ID),
    CONSTRAINT fk_dest FOREIGN KEY (DEST_FACILITY_ID) REFERENCES FACILITIES(FACILITY_ID)
);