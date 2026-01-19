CREATE OR REPLACE VIEW edwclm_bobj_views.fact_claim_physician
AS
	SELECT
		fd.company_code,
		fd.coid,
		a.claim_id,
		a.phys_type_code,
		a.phys_qual_code,
		a.phys_code,
		a.phys_last_name,
		a.phys_first_name,
		a.phys_taxonomy_code,
		a.dw_last_update_date_time,
		a.source_system_code
	FROM edwclm_base_views.fact_claim_physician AS a
    	INNER JOIN edwclm_base_views.fact_claim AS c
		ON a.claim_id = c.claim_id
	INNER JOIN edwclm_base_views.facility_dimension AS fd
		ON fd.unit_num = c.unit_num
;
