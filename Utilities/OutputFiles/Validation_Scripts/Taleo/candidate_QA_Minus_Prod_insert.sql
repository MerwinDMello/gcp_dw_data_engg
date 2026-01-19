SELECT 
	candidate_sid,
	candidate_num,
	in_hiring_process_sw,
	internal_candidate_sw,
	referred_sw,
	last_modified_date_time,
	candidate_creation_date_time,
	residence_location_num,
	travel_preference_code,
	relocation_preference_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate
Where DATE(dw_last_update_date_time) = '2023-03-27'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_sid,
	candidate_num,
	in_hiring_process_sw,
	internal_candidate_sw,
	referred_sw,
	last_modified_date_time,
	candidate_creation_date_time,
	residence_location_num,
	travel_preference_code,
	relocation_preference_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'