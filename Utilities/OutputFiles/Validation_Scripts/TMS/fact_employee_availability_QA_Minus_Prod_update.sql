SELECT 
	employee_talent_profile_sid,
	employee_sid,
	jobs_pooled_for_cnt,
	employee_talent_pool_cnt,
	employee_successor_cnt,
	employee_ready_now_cnt,
	employee_ready_18_24_month_cnt,
	employee_ready_12_18_month_cnt,
	employee_ready_6_11_month_cnt,
	employee_other_readiness_cnt,
	employee_readiness_unknown_cnt,
	employee_slated_for_position_cnt,
	employee_talent_pooled_for_position_cnt,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.fact_employee_availability
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
EXCEPT DISTINCT
SELECT 
	employee_talent_profile_sid,
	employee_sid,
	jobs_pooled_for_cnt,
	employee_talent_pool_cnt,
	employee_successor_cnt,
	employee_ready_now_cnt,
	employee_ready_18_24_month_cnt,
	employee_ready_12_18_month_cnt,
	employee_ready_6_11_month_cnt,
	employee_other_readiness_cnt,
	employee_readiness_unknown_cnt,
	employee_slated_for_position_cnt,
	employee_talent_pooled_for_position_cnt,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.fact_employee_availability
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'