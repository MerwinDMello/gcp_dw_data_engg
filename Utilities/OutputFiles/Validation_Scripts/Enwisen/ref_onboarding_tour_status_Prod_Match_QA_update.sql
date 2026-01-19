SELECT 
	tour_status_id,
	tour_status_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_onboarding_tour_status
Where DATE(dw_last_update_date_time) = '2023-01-21'
INTERSECT DISTINCT
SELECT 
	tour_status_id,
	tour_status_text,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_onboarding_tour_status
Where DATE(dw_last_update_date_time) = '2023-01-25'