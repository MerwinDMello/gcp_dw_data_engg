CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_treatment_regimen
   OPTIONS(description='Contains treatment regimen details for Hematology patient')
  AS SELECT
      cn_patient_heme_treatment_regimen.cn_patient_heme_diagnosis_sid,
      cn_patient_heme_treatment_regimen.nav_patient_id,
      cn_patient_heme_treatment_regimen.regimen_id,
      cn_patient_heme_treatment_regimen.treatment_phase_id,
      cn_patient_heme_treatment_regimen.pathway_var_reason_id,
      cn_patient_heme_treatment_regimen.tumor_type_id,
      cn_patient_heme_treatment_regimen.diagnosis_result_id,
      cn_patient_heme_treatment_regimen.nav_diagnosis_id,
      cn_patient_heme_treatment_regimen.navigator_id,
      cn_patient_heme_treatment_regimen.coid,
      cn_patient_heme_treatment_regimen.company_code,
      cn_patient_heme_treatment_regimen.planned_start_date,
      cn_patient_heme_treatment_regimen.actual_start_date,
      cn_patient_heme_treatment_regimen.drug_text,
      cn_patient_heme_treatment_regimen.cycle_num,
      cn_patient_heme_treatment_regimen.cycle_length_num,
      cn_patient_heme_treatment_regimen.cycle_frequency_text,
      cn_patient_heme_treatment_regimen.ordinal_cycle_num,
      cn_patient_heme_treatment_regimen.pathway_ind,
      cn_patient_heme_treatment_regimen.pathway_text,
      cn_patient_heme_treatment_regimen.pathway_compliant_ind,
      cn_patient_heme_treatment_regimen.treatment_plan_document_date,
      cn_patient_heme_treatment_regimen.prior_plan_document_timeframe_ind,
      cn_patient_heme_treatment_regimen.treatment_regimen_comment_text,
      cn_patient_heme_treatment_regimen.hashbite_ssk,
      cn_patient_heme_treatment_regimen.source_system_code,
      cn_patient_heme_treatment_regimen.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_treatment_regimen
  ;
