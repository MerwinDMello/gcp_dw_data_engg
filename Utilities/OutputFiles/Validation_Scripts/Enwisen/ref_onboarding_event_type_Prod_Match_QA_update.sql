SELECT 
	event_type_id,
	event_type_code,
	event_type_desc,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_onboarding_event_type
Where DATE(dw_last_update_date_time) = '2023-01-21'
INTERSECT DISTINCT
SELECT 
	event_type_id,
	event_type_code,
	event_type_desc,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_onboarding_event_type
Where DATE(dw_last_update_date_time) = '2023-01-25'