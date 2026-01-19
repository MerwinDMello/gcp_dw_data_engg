CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient_toxicity
   OPTIONS(description='Contains fact information of Radiation Oncology for patient toxicity')
  AS SELECT
      fact_rad_onc_patient_toxicity.fact_patient_toxicity_sk,
      fact_rad_onc_patient_toxicity.toxicity_component_sk,
      fact_rad_onc_patient_toxicity.toxicity_assessment_type_sk,
      fact_rad_onc_patient_toxicity.activity_transaction_sk,
      fact_rad_onc_patient_toxicity.patient_sk,
      fact_rad_onc_patient_toxicity.scheme_id,
      fact_rad_onc_patient_toxicity.toxicity_cause_certainty_type_id,
      fact_rad_onc_patient_toxicity.toxicity_cause_type_id,
      fact_rad_onc_patient_toxicity.diagnosis_code_sk,
      fact_rad_onc_patient_toxicity.site_sk,
      fact_rad_onc_patient_toxicity.source_fact_patient_toxicity_id,
      fact_rad_onc_patient_toxicity.assessment_date_time,
      fact_rad_onc_patient_toxicity.toxicity_effective_date,
      fact_rad_onc_patient_toxicity.toxicity_grade_num,
      fact_rad_onc_patient_toxicity.valid_entry_ind,
      fact_rad_onc_patient_toxicity.toxicity_approved_date_time,
      fact_rad_onc_patient_toxicity.assessment_performed_date_time,
      fact_rad_onc_patient_toxicity.toxicity_reason_text,
      fact_rad_onc_patient_toxicity.toxicity_approved_ind,
      fact_rad_onc_patient_toxicity.toxicity_header_valid_entry_ind,
      fact_rad_onc_patient_toxicity.revision_num,
      fact_rad_onc_patient_toxicity.log_id,
      fact_rad_onc_patient_toxicity.run_id,
      fact_rad_onc_patient_toxicity.source_system_code,
      fact_rad_onc_patient_toxicity.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_toxicity
  ;
