-- The table has over 4000 partitions; 
-- A simple Insert Select job will not work. So, it is split into two Insert Selects
-- https://docs.cloud.google.com/bigquery/quotas#partitioned_tables

TRUNCATE TABLE edwpsc.artiva_stggcpoolgeninfo_history;

INSERT INTO edwpsc.artiva_stggcpoolgeninfo_history
SELECT *
FROM `prod_support.artiva_stggcpoolgeninfo_history`
WHERE snapshot_date 
BETWEEN '2010-01-01T00:00:00' AND '2020-12-31T23:59:59';

INSERT INTO edwpsc.artiva_stggcpoolgeninfo_history
SELECT *
FROM `prod_support.artiva_stggcpoolgeninfo_history`
WHERE snapshot_date > '2020-12-31T23:59:59';