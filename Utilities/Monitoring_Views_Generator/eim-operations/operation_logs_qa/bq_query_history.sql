CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_qa.bq_query_history`
AS
SELECT * FROM hca-hin-qa-cur-hr.edwhr_ac.bq_query_history
UNION ALL
SELECT * FROM hca-hin-qa-cur-ops.edwops_ac.bq_query_history
UNION ALL
SELECT * FROM hca-hin-qa-cur-clinical.edwclin_ac.bq_query_history