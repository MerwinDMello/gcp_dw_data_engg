SELECT 
	employee_work_history_sid,
	employee_talent_profile_sid,
	employee_sid,
	previous_work_address_sid,
	work_history_company_name,
	work_history_job_title_text,
	work_history_desc,
	work_history_start_date,
	work_history_end_date,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.employee_work_history
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
INTERSECT DISTINCT
SELECT 
	employee_work_history_sid,
	employee_talent_profile_sid,
	employee_sid,
	previous_work_address_sid,
	work_history_company_name,
	work_history_job_title_text,
	work_history_desc,
	work_history_start_date,
	work_history_end_date,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.employee_work_history
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'