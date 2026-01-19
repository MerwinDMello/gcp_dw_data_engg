SELECT 
	recruitment_job_sid,
	recruitment_job_num,
	job_title_name,
	job_grade_code,
	job_schedule_id,
	overtime_status_id,
	primary_facility_location_num,
	recruiter_user_sid,
	recruitment_job_parameter_sid,
	recruitment_position_sid,
	fte_pct,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.recruitment_job
Where DATE(dw_last_update_date_time) = '2023-03-28'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	recruitment_job_sid,
	recruitment_job_num,
	job_title_name,
	job_grade_code,
	job_schedule_id,
	overtime_status_id,
	primary_facility_location_num,
	recruiter_user_sid,
	recruitment_job_parameter_sid,
	recruitment_position_sid,
	fte_pct,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.recruitment_job
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'