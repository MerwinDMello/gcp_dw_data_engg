CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs.bq_query_cost`
AS
SELECT * FROM hca-hin-prod-cur-hr.edwhr_ac.bq_query_cost
UNION ALL
SELECT * FROM hca-hin-prod-cur-ops.edwops_ac.bq_query_cost;