CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_qa.composer_task_status`
AS
SELECT * FROM hca-hin-qa-cur-hr.edwhr_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-qa-cur-ops.edwops_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-qa-cur-clinical.edwclin_ac.composer_task_status