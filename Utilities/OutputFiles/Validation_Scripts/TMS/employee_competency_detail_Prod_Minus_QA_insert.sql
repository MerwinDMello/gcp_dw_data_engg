SELECT 
	employee_competency_result_sid,
	employee_talent_profile_sid,
	employee_sid,
	performance_plan_id,
	competency_group_id,
	competency_id,
	evaluation_workflow_status_id,
	review_period_id,
	review_year_num,
	review_period_start_date,
	review_period_end_date,
	employee_rating_num,
	employee_rating_id,
	manager_rating_num,
	manager_rating_id,
	manager_employee_rating_gap_num,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.employee_competency_detail
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	employee_competency_result_sid,
	employee_talent_profile_sid,
	employee_sid,
	performance_plan_id,
	competency_group_id,
	competency_id,
	evaluation_workflow_status_id,
	review_period_id,
	review_year_num,
	review_period_start_date,
	review_period_end_date,
	employee_rating_num,
	employee_rating_id,
	manager_rating_num,
	manager_rating_id,
	manager_employee_rating_gap_num,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.employee_competency_detail
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) = '9999-12-31'