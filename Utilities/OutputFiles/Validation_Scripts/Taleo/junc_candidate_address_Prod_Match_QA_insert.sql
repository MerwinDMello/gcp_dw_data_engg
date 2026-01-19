SELECT 
	candidate_sid,
	addr_sid,
	addr_type_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.junc_candidate_address
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'
INTERSECT DISTINCT
SELECT 
	candidate_sid,
	addr_sid,
	addr_type_code,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.junc_candidate_address
Where DATE(dw_last_update_date_time) = '2023-03-29'
AND DATE(VALID_TO_DATE) = '9999-12-31'