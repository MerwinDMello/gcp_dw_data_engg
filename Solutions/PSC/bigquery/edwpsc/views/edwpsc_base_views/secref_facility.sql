CREATE OR REPLACE VIEW edwpsc_base_views.secref_facility
AS
SELECT
	company_code,
	user_id,
	co_id
FROM 
	edwpsc_base_views.hin_secref_facility
UNION DISTINCT
SELECT
	company_code,
	user_id,
	co_id
FROM {{ params.param_pub_project_id }}.auth_base_views.secref_facility
WHERE line_of_business = 'edwpsc'
;