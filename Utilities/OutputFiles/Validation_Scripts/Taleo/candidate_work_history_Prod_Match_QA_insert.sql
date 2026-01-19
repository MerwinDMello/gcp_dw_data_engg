SELECT 
	candidate_work_history_sid,
	candidate_work_history_num,
	candidate_profile_sid,
	candidate_sid,
	work_start_date,
	work_end_date,
	current_employer_sw,
	profile_display_seq_num,
	employer_name,
	job_title_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_work_history
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'
INTERSECT DISTINCT
SELECT 
	candidate_work_history_sid,
	candidate_work_history_num,
	candidate_profile_sid,
	candidate_sid,
	work_start_date,
	work_end_date,
	current_employer_sw,
	profile_display_seq_num,
	employer_name,
	job_title_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_work_history
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) = '9999-12-31'