CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_qa.bq_query_cost`
AS
SELECT * FROM hca-hin-qa-cur-hr.edwhr_ac.bq_query_cost
UNION ALL
SELECT * FROM hca-hin-qa-cur-ops.edwops_ac.bq_query_cost
UNION ALL
SELECT * FROM hca-hin-qa-cur-clinical.edwclin_ac.bq_query_cost