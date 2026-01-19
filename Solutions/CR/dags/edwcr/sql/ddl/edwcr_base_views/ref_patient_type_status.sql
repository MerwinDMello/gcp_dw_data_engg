CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_patient_type_status
   OPTIONS(description='This table will store the patient type status for cancer patient Id')
  AS SELECT
      ref_patient_type_status.patient_type_status_sk,
      ref_patient_type_status.patient_type_status_code,
      ref_patient_type_status.patient_type_status_desc,
      ref_patient_type_status.source_system_code,
      ref_patient_type_status.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_patient_type_status
  ;
