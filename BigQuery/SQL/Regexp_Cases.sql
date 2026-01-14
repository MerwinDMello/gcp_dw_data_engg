-- Extract Table Name from Qualified Table Name
SELECT REGEXP_EXTRACT('edwim_staging.Test_Table1', r'^[a-zA-Z_]+\.([a-zA-Z0-9_.]+$)')

-- Flip Given and First Names
SELECT 
CONCAT(REGEXP_EXTRACT('DMello Merwin', r'^\S+\s+(\S+)$'),' ',REGEXP_EXTRACT('DMello Merwin', r'^(\S+)\s+\S+$'))

-- Extract Date Elements
SELECT parse_date('%Y-%m-%d',REGEXP_EXTRACT('2025-02-28 00:00:00', r'[0-9]+-[0-9]+-[0-9]+'));
-- Output: 2025-02-28

SELECT parse_date('%m/%d/%Y',REGEXP_EXTRACT('02/20/2006', r'[0-9]+/[0-9]+/[0-9]+'));
-- Output: 2006-02-20

-- Extract Time Elements : Convert 12 Hour Notation to 24 Hour Notation
SELECT 
parse_datetime('%x %I:%M %p',REGEXP_EXTRACT('10/28/24 1:58 PM EST', r'[0-9]+/[0-9]+/[0-9]+[\x20][0-9]+:[0-9]+[\x20][A-Z|a-z]+')) as date_time_1,
parse_datetime('%m/%d/%y %I:%M %p',REGEXP_EXTRACT('10/28/24 1:58 PM EST', r'[0-9]+/[0-9]+/[0-9]+[\x20][0-9]+:[0-9]+[\x20][A-Z|a-z]+')) as date_time_2
-- Output 2024-10-28T13:58:00

-- Extract substrings if there is a match
SELECT REGEXP_EXTRACT(TRIM(LOWER(dag_name)),r'^.*(daily|weekly|monthly|quarterly).*$')