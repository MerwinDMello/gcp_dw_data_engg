CREATE OR REPLACE VIEW `hca-eim-operations.operation_logs_dev.audit_control_combined`
AS
SELECT * FROM hca-hin-dev-cur-hr.edwhr_ac.audit_control_combined
UNION ALL
SELECT * FROM hca-hin-dev-cur-ops.edwops_ac.audit_control_combined
UNION ALL
SELECT * FROM hca-hin-dev-cur-clinical.edwclin_ac.audit_control_combined
UNION ALL
SELECT * FROM hca-hin-dev-cur-pub.edw_sec_base_views.audit_control_combined