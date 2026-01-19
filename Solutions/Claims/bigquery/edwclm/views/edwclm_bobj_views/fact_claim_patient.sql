CREATE OR REPLACE VIEW edwclm_bobj_views.fact_claim_patient
AS
	SELECT
		fd.company_code,
		fd.coid,
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
	FROM edwclm_base_views.fact_claim_patient AS a
	INNER JOIN edwclm_base_views.fact_claim AS c
		ON a.claim_id = c.claim_id
	INNER JOIN edwclm_base_views.facility_dimension AS fd
		ON fd.unit_num = c.unit_num
;
