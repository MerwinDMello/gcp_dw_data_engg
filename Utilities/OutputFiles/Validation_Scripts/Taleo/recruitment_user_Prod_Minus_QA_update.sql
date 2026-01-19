SELECT 
	recruitment_user_sid,
	employee_num,
	employee_34_login_code,
	recruitment_user_num,
	first_name,
	last_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.recruitment_user
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
EXCEPT DISTINCT
SELECT 
	recruitment_user_sid,
	employee_num,
	employee_34_login_code,
	recruitment_user_num,
	first_name,
	last_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.recruitment_user
Where DATE(dw_last_update_date_time) = '2023-03-27'
AND DATE(VALID_TO_DATE) <> '9999-12-31'