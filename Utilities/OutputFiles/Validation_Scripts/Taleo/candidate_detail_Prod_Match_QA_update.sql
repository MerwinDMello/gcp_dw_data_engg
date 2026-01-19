SELECT 
	candidate_sid,
	element_detail_entity_text,
	element_detail_type_text,
	element_detail_seq,
	element_detail_id,
	element_detail_value_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.candidate_detail
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) <> '9999-12-31'
INTERSECT DISTINCT
SELECT 
	candidate_sid,
	element_detail_entity_text,
	element_detail_type_text,
	element_detail_seq,
	element_detail_id,
	element_detail_value_text,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.candidate_detail
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) <> '9999-12-31'