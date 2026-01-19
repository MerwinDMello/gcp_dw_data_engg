SELECT 
	candidate_onboarding_event_sid,
	event_type_id,
	recruitment_requisition_num_text,
	completed_date,
	candidate_sid,
	resource_screening_package_num,
	sequence_num,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.candidate_onboarding_event
Where DATE(dw_last_update_date_time) = '2023-01-25'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_onboarding_event_sid,
	event_type_id,
	recruitment_requisition_num_text,
	completed_date,
	candidate_sid,
	resource_screening_package_num,
	sequence_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_onboarding_event
Where DATE(dw_last_update_date_time) = '2023-01-21'
AND DATE(VALID_TO_DATE) = '9999-12-31'