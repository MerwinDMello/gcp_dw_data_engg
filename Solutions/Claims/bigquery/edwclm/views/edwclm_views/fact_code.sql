CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.fact_code
AS SELECT
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
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_code AS a
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.fact_claim AS c ON upper(rtrim(a.claim_id, ' ')) = upper(rtrim(c.claim_id, ' '))
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd ON rtrim(fd.unit_num, ' ') = rtrim(c.unit_num, ' ')
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.secref_facility AS b ON rtrim(fd.coid, ' ') = rtrim(b.co_id, ' ')
	AND rtrim(fd.company_code, ' ') = rtrim(b.company_code, ' ')
    AND rtrim(b.user_id, ' ') = rtrim(session_user(), ' ')
;
