SELECT 
	employee_education_profile_sid,
	employee_education_type_code,
	employee_talent_profile_sid,
	employee_sid,
	detail_value_alpahnumeric_text,
	detail_value_num,
	detail_value_date,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.employee_education_profile
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
INTERSECT DISTINCT
SELECT 
	employee_education_profile_sid,
	employee_education_type_code,
	employee_talent_profile_sid,
	employee_sid,
	detail_value_alpahnumeric_text,
	detail_value_num,
	detail_value_date,
	employee_num,
	lawson_company_num,
	process_level_code,
	source_system_key,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.employee_education_profile
Where DATE(dw_last_update_date_time) = '2023-06-22'
AND DATE(VALID_TO_DATE) <> '9999-12-31'