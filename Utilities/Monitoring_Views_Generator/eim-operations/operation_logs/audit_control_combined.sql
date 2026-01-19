CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs.audit_control_combined`
AS
SELECT * FROM hca-hin-prod-cur-hr.edwhr_ac.audit_control_combined
UNION ALL
SELECT * FROM hca-hin-prod-cur-ops.edwops_ac.audit_control_combined
UNION ALL
SELECT * FROM hca-hin-prod-cur-clinical.edwclin_ac.audit_control_combined