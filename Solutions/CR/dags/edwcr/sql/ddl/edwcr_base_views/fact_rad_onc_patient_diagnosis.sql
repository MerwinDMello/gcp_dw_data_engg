CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient_diagnosis
   OPTIONS(description='Contains fact information of Radiation Oncology for a patient diagnosis')
  AS SELECT
      fact_rad_onc_patient_diagnosis.fact_patient_diagnosis_sk,
      fact_rad_onc_patient_diagnosis.diagnosis_code_sk,
      fact_rad_onc_patient_diagnosis.patient_sk,
      fact_rad_onc_patient_diagnosis.diagnosis_status_id,
      fact_rad_onc_patient_diagnosis.cell_category_id,
      fact_rad_onc_patient_diagnosis.cell_grade_id,
      fact_rad_onc_patient_diagnosis.laterality_id,
      fact_rad_onc_patient_diagnosis.stage_id,
      fact_rad_onc_patient_diagnosis.stage_status_id,
      fact_rad_onc_patient_diagnosis.recurrence_id,
      fact_rad_onc_patient_diagnosis.invasion_id,
      fact_rad_onc_patient_diagnosis.confirmed_diagnosis_id,
      fact_rad_onc_patient_diagnosis.diagnosis_type_id,
      fact_rad_onc_patient_diagnosis.site_sk,
      fact_rad_onc_patient_diagnosis.source_fact_patient_diagnosis_id,
      fact_rad_onc_patient_diagnosis.diagnosis_status_date,
      fact_rad_onc_patient_diagnosis.diagnosis_text,
      fact_rad_onc_patient_diagnosis.clinical_text,
      fact_rad_onc_patient_diagnosis.pathology_comment_text,
      fact_rad_onc_patient_diagnosis.node_num,
      fact_rad_onc_patient_diagnosis.positive_node_num,
      fact_rad_onc_patient_diagnosis.log_id,
      fact_rad_onc_patient_diagnosis.run_id,
      fact_rad_onc_patient_diagnosis.source_system_code,
      fact_rad_onc_patient_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.fact_rad_onc_patient_diagnosis
  ;
