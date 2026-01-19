CREATE OR REPLACE VIEW {{ params.param_clm_views_dataset_name }}.fact_claim_patient
AS SELECT
		a.claim_id,
		a.patient_last_name,
		a.patient_first_name,
		a.patient_addr1,
		a.patient_addr2,
		a.patient_city,
		a.patient_st,
		a.patient_zip_cd,
		a.patient_sex_cd,
		a.patient_dob,
		a.resp_party_name,
		a.resp_party_addr1,
		a.resp_party_addr2,
		a.resp_party_city,
		a.resp_party_st,
		a.resp_party_zip_cd,
		a.dw_last_update_date_time,
		a.source_system_code
  FROM
    {{ params.param_clm_base_views_dataset_name }}.fact_claim_patient AS a
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.fact_claim AS c ON upper(rtrim(a.claim_id, ' ')) = upper(rtrim(c.claim_id, ' '))
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.facility_dimension AS fd ON rtrim(fd.unit_num, ' ') = rtrim(c.unit_num, ' ')
    INNER JOIN {{ params.param_clm_base_views_dataset_name }}.secref_facility AS b ON rtrim(fd.coid, ' ') = rtrim(b.co_id, ' ')
	AND rtrim(fd.company_code, ' ') = rtrim(b.company_code, ' ')
    AND rtrim(b.user_id, ' ') = rtrim(session_user(), ' ')
;
