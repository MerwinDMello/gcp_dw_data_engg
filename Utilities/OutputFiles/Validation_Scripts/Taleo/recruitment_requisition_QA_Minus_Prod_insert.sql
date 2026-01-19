SELECT 
	recruitment_requisition_sid,
	requisition_num,
	lawson_requisition_sid,
	lawson_requisition_num,
	hiring_manager_user_sid,
	recruitment_requisition_num_text,
	process_level_code,
	approved_sw,
	target_start_date,
	required_asset_num,
	required_asset_sw,
	workflow_id,
	recruitment_job_sid,
	job_template_sid,
	requisition_new_graduate_flag,
	lawson_company_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr_copy.recruitment_requisition
Where DATE(dw_last_update_date_time) = '2023-03-28'
AND DATE(VALID_TO_DATE) = '9999-12-31'
EXCEPT DISTINCT
SELECT 
	recruitment_requisition_sid,
	requisition_num,
	lawson_requisition_sid,
	lawson_requisition_num,
	hiring_manager_user_sid,
	recruitment_requisition_num_text,
	process_level_code,
	approved_sw,
	target_start_date,
	required_asset_num,
	required_asset_sw,
	workflow_id,
	recruitment_job_sid,
	job_template_sid,
	requisition_new_graduate_flag,
	lawson_company_num,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwhr.recruitment_requisition
Where DATE(dw_last_update_date_time) = '2023-03-26'
AND DATE(VALID_TO_DATE) = '9999-12-31'