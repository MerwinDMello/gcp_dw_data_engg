CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_dev.bq_query_history`
AS
SELECT * FROM hca-hin-dev-cur-hr.edwhr_ac.bq_query_history
UNION ALL
SELECT * FROM hca-hin-dev-cur-ops.edwops_ac.bq_query_history
UNION ALL
SELECT * FROM hca-hin-dev-cur-clinical.edwclin_ac.bq_query_history