CREATE OR REPLACE VIEW edwclm_bobj_views.fact_code
AS
	SELECT
		fd.company_code,
		fd.coid,
		a.claim_id,
		a.code_seq_num,
		a.code_type_id,
		a.code_value,
		a.code_amt,
		a.code_from_dt,
		a.code_thru_dt,
		a.code_poa_ind,
		a.dw_last_update_date_time,
		a.source_system_code
	FROM edwclm_base_views.fact_code AS a
	INNER JOIN edwclm_base_views.fact_claim AS c
		ON a.claim_id = c.claim_id
	INNER JOIN edwclm_base_views.facility_dimension AS fd
		ON fd.unit_num = c.unit_num 
;
