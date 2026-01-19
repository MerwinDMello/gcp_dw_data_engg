SELECT 
	email_sent_status_id,
	email_sent_status_text,
	hr_status_desc,
	source_system_code,
FROM
hca-hin-qa-cur-hr.edwhr.ref_email_to_hr_status
Where DATE(dw_last_update_date_time) = '2023-01-25'
EXCEPT DISTINCT
SELECT 
	email_sent_status_id,
	email_sent_status_text,
	hr_status_desc,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_email_to_hr_status
Where DATE(dw_last_update_date_time) = '2023-01-21'