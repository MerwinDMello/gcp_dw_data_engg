SELECT 
	candidate_sid,
	first_name,
	middle_name,
	last_name,
	maiden_name,
	social_security_num,
	email_address,
	birth_date,
	driver_license_num,
	driver_license_state_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_person
Where DATE(dw_last_update_date_time) = '2023-03-27'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_sid,
	first_name,
	middle_name,
	last_name,
	maiden_name,
	social_security_num,
	email_address,
	birth_date,
	driver_license_num,
	driver_license_state_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_person
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) <> '9999-12-31'