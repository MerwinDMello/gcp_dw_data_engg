SELECT 
	answer_sid,
	answer_num,
	answer_desc,
	question_sid,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_answer
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
INTERSECT DISTINCT
SELECT 
	answer_sid,
	answer_num,
	answer_desc,
	question_sid,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_answer
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) <> '9999-12-31'