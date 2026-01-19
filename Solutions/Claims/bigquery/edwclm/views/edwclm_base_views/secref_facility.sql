CREATE OR REPLACE VIEW {{ params.param_clm_base_views_dataset_name }}.secref_facility
AS
SELECT
    secref_facility.company_code,
    secref_facility.user_id,
    secref_facility.co_id
FROM {{ params.param_mirroring_project_id }}.{{ params.param_clm_mirrored_core_dataset_name }}.secref_facility
UNION DISTINCT
	SELECT
		company_code,
		user_id,
		co_id
	FROM 
	{{ params.param_clm_base_views_dataset_name }}.hin_secref_facility
UNION DISTINCT
SELECT
	company_code,
	user_id,
	co_id
FROM {{ params.param_pub_project_id }}.{{ params.param_auth_base_views_dataset_name }}.secref_facility
WHERE line_of_business = 'edwclm'
;