SELECT 
	candidate_onboarding_sid,
	requisition_sid,
	employee_sid,
	candidate_sid,
	candidate_first_name,
	candidate_last_name,
	tour_start_date,
	tour_id,
	tour_status_id,
	tour_completion_pct,
	workflow_id,
	workflow_status_id,
	email_sent_status_id,
	onboarding_confirmation_date,
	recruitment_requisition_num_text,
	process_level_code,
	applicant_num,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.candidate_onboarding
Where DATE(dw_last_update_date_time) = '2023-01-25'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	candidate_onboarding_sid,
	requisition_sid,
	employee_sid,
	candidate_sid,
	candidate_first_name,
	candidate_last_name,
	tour_start_date,
	tour_id,
	tour_status_id,
	tour_completion_pct,
	workflow_id,
	workflow_status_id,
	email_sent_status_id,
	onboarding_confirmation_date,
	recruitment_requisition_num_text,
	process_level_code,
	applicant_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_onboarding
Where DATE(dw_last_update_date_time) = '2023-01-21'
AND DATE(VALID_TO_DATE) = '9999-12-31'