CREATE OR REPLACE VIEW {{ params.param_clm_mirrored_base_views_dataset_name }}.fact_claim_patient
AS SELECT
		fact_claim_patient.claim_id,
		fact_claim_patient.patient_last_name,
		fact_claim_patient.patient_first_name,
		fact_claim_patient.patient_addr1,
		fact_claim_patient.patient_addr2,
		fact_claim_patient.patient_city,
		fact_claim_patient.patient_st,
		fact_claim_patient.patient_zip_cd,
		fact_claim_patient.patient_sex_cd,
		fact_claim_patient.patient_dob,
		fact_claim_patient.resp_party_name,
		fact_claim_patient.resp_party_addr1,
		fact_claim_patient.resp_party_addr2,
		fact_claim_patient.resp_party_city,
		fact_claim_patient.resp_party_st,
		fact_claim_patient.resp_party_zip_cd,
		fact_claim_patient.dw_last_update_date_time,
		fact_claim_patient.source_system_code
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_clm_mirrored_core_dataset_name }}.fact_claim_patient
;
