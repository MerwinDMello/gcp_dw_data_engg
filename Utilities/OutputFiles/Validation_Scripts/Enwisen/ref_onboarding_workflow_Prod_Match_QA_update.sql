SELECT 
	workflow_id,
	workflow_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_onboarding_workflow
Where DATE(dw_last_update_date_time) = '2023-01-21'
INTERSECT DISTINCT
SELECT 
	workflow_id,
	workflow_name,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_onboarding_workflow
Where DATE(dw_last_update_date_time) = '2023-01-25'