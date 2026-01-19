SELECT 
	question_answer_sid,
	question_answer_num,
	candidate_sid,
	creation_date,
	question_sid,
	answer_sid,
	comment_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_question_answer
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	question_answer_sid,
	question_answer_num,
	candidate_sid,
	creation_date,
	question_sid,
	answer_sid,
	comment_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_question_answer
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'