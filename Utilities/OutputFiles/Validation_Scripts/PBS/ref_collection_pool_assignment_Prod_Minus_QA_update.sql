SELECT 
	pool_assignment_id,
	artiva_instance_code,
	pool_assignment_desc,
	pool_category_desc,
	active_ind,
	dialer_name,
	dialing_status_code,
	inclusion_flag,
	pool_assigned_location_name,
	source_system_code,
FROM
hca-hin-dev-cur-hr.edwpbs_base_views.ref_collection_pool_assignment
Where DATE(dw_last_update_date_time) = '2023-12-21'
EXCEPT DISTINCT
SELECT 
	pool_assignment_id,
	artiva_instance_code,
	pool_assignment_desc,
	pool_category_desc,
	active_ind,
	dialer_name,
	dialing_status_code,
	inclusion_flag,
	pool_assigned_location_name,
	source_system_code,
FROM
hca-hin-prod-cur-hr.edwpbs.ref_collection_pool_assignment
Where DATE(dw_last_update_date_time) = '2023-12-21'