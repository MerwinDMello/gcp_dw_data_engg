SELECT 
	candidate_profile_sid,
	candidate_sid,
	profile_medium_id,
	candidate_profile_num,
	submission_date,
	completion_date,
	creation_date,
	recruitment_source_id,
	recruitment_source_auto_filled_sw,
	requisition_num,
	job_application_num,
	candidate_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_profile
Where DATE(dw_last_update_date_time) = '2023-03-28'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_profile_sid,
	candidate_sid,
	profile_medium_id,
	candidate_profile_num,
	submission_date,
	completion_date,
	creation_date,
	recruitment_source_id,
	recruitment_source_auto_filled_sw,
	requisition_num,
	job_application_num,
	candidate_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_profile
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) <> '9999-12-31'