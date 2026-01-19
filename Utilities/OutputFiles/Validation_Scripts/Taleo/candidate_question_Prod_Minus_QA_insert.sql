SELECT 
	question_sid,
	question_num,
	creation_date,
	question_desc,
	question_code,
	last_modified_date,
	requisition_num,
	question_type_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_question
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	question_sid,
	question_num,
	creation_date,
	question_desc,
	question_code,
	last_modified_date,
	requisition_num,
	question_type_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_question
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) = '9999-12-31'