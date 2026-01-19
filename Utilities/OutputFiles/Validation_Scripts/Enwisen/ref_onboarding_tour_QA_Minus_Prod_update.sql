SELECT 
	tour_id,
	tour_name,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_onboarding_tour
Where DATE(dw_last_update_date_time) = '2023-01-25'
EXCEPT DISTINCT
SELECT 
	tour_id,
	tour_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_onboarding_tour
Where DATE(dw_last_update_date_time) = '2023-01-21'