CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs.bq_query_history`
AS
SELECT * FROM hca-hin-prod-cur-hr.edwhr_ac.bq_query_history
UNION ALL
SELECT * FROM hca-hin-prod-cur-ops.edwops_ac.bq_query_history;