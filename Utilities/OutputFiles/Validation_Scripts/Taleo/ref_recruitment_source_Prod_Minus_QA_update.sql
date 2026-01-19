SELECT 
	recruitment_source_id,
	recruitment_source_desc,
	recruitment_source_type_id,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_recruitment_source
Where DATE(dw_last_update_date_time) = '2023-06-14'
EXCEPT DISTINCT
SELECT 
	recruitment_source_id,
	recruitment_source_desc,
	recruitment_source_type_id,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.ref_recruitment_source
Where DATE(dw_last_update_date_time) = '2023-06-14'