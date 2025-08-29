-- Migration script to add broker percentage fields
-- Run this script to add the new fields to your existing import_summaries table

USE logcomex;

-- Add the three new broker percentage fields
ALTER TABLE import_summaries 
ADD COLUMN pct_broker_3995 DECIMAL(5,2) DEFAULT 0.00 AFTER num_custom_brokers_used,
ADD COLUMN pct_broker_3714 DECIMAL(5,2) DEFAULT 0.00 AFTER pct_broker_3995,
ADD COLUMN pct_broker_1720 DECIMAL(5,2) DEFAULT 0.00 AFTER pct_broker_3714;

-- Verify the fields were added
DESCRIBE import_summaries;

-- Show the new structure
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'logcomex' 
AND TABLE_NAME = 'import_summaries' 
AND COLUMN_NAME IN ('pct_broker_3995', 'pct_broker_3714', 'pct_broker_1720');
