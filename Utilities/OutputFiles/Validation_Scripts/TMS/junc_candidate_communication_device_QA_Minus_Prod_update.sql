SELECT 
	candidate_sid,
	communication_device_sid,
	communication_device_type_code,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.junc_candidate_communication_device
Where DATE(dw_last_update_date_time) = '2023-08-10'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_sid,
	communication_device_sid,
	communication_device_type_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_base_views.junc_candidate_communication_device
Where DATE(dw_last_update_date_time) = '2023-08-10'
AND DATE(VALID_TO_DATE) <> '9999-12-31'