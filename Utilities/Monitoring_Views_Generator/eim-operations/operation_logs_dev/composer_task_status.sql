CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_dev.composer_task_status`
AS
SELECT * FROM hca-hin-dev-cur-hr.edwhr_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-dev-cur-ops.edwops_ac.composer_task_status
UNION ALL
SELECT * FROM hca-hin-dev-cur-clinical.edwclin_ac.composer_task_status