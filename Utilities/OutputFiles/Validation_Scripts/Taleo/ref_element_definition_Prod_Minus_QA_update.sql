SELECT 
	element_detail_entity_text,
	element_detail_type_text,
	element_detail_definition_desc,
	element_detail_definition_type_id,
	element_definition_selection_id,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.ref_element_definition
Where DATE(dw_last_update_date_time) = '2023-06-14'
EXCEPT DISTINCT
SELECT 
	element_detail_entity_text,
	element_detail_type_text,
	element_detail_definition_desc,
	element_detail_definition_type_id,
	element_definition_selection_id,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwhr.ref_element_definition
Where DATE(dw_last_update_date_time) = '2023-06-14'