CREATE OR REPLACE VIEW {{ params.param_rmt_mirrored_base_views_dataset_name }}.fact_patient_remit
AS SELECT
	fact_patient_remit.patient_remit_sid,
	fact_patient_remit.remit_id,
	fact_patient_remit.remit_provider_id,
	fact_patient_remit.remit_payer_id,
	fact_patient_remit.patient_acct_num,
	fact_patient_remit.unit_num,
	fact_patient_remit.iplan,
	fact_patient_remit.pas_coid,
	fact_patient_remit.remit_file_path,
	fact_patient_remit.remit_file_name,
	fact_patient_remit.claim_file_name,
	fact_patient_remit.remit_bill_dt,
	fact_patient_remit.remit_claim_status_cd,
	fact_patient_remit.remit_payment_amt,
	fact_patient_remit.remit_claim_id,
	fact_patient_remit.tot_claim_charge_amt,
	fact_patient_remit.patient_resp_amt,
	fact_patient_remit.claim_file_inc_cd,
	fact_patient_remit.payer_claim_ctrll_num,
	fact_patient_remit.facility_cd,
	fact_patient_remit.claim_freq_type_cd,
	fact_patient_remit.drg,
	fact_patient_remit.drg_weight,
	fact_patient_remit.discharge_fraction,
	fact_patient_remit.dw_last_update_date_time,
	fact_patient_remit.source_system_code,
	fact_patient_remit.customer_cd
  FROM
    {{ params.param_mirroring_project_id }}.{{ params.param_rmt_mirrored_core_dataset_name }}.fact_patient_remit
;
