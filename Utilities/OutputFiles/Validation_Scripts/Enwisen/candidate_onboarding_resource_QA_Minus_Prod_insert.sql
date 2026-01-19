SELECT 
	resource_screening_package_num,
	candidate_sid,
	recruitment_requisition_sid,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.candidate_onboarding_resource
Where DATE(dw_last_update_date_time) = '2023-01-25'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	resource_screening_package_num,
	candidate_sid,
	recruitment_requisition_sid,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_onboarding_resource
Where DATE(dw_last_update_date_time) = '2023-01-21'
AND DATE(VALID_TO_DATE) = '9999-12-31'