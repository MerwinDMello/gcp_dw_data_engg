CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs.composer_task_status`
AS
SELECT * FROM hca-hin-prod-cur-hr.edwhr_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-prod-cur-ops.edwops_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-prod-cur-clinical.edwclin_ac.composer_task_status