SELECT 
	workflow_status_id,
	workflow_status_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_onboarding_workflow_status
Where DATE(dw_last_update_date_time) = '2023-01-21'
EXCEPT DISTINCT
SELECT 
	workflow_status_id,
	workflow_status_text,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_onboarding_workflow_status
Where DATE(dw_last_update_date_time) = '2023-01-25'