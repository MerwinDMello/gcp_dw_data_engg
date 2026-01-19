CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery_reconstruction
   OPTIONS(description='Contains the details behind any breast reconstructive surgery.')
  AS SELECT
      cn_patient_surgery_reconstruction.cn_patient_surgery_recstr_sid,
      cn_patient_surgery_reconstruction.core_record_type_id,
      cn_patient_surgery_reconstruction.surgery_recstr_side_id,
      cn_patient_surgery_reconstruction.med_spcl_physician_id,
      cn_patient_surgery_reconstruction.nav_patient_id,
      cn_patient_surgery_reconstruction.tumor_type_id,
      cn_patient_surgery_reconstruction.diagnosis_result_id,
      cn_patient_surgery_reconstruction.nav_diagnosis_id,
      cn_patient_surgery_reconstruction.navigator_id,
      cn_patient_surgery_reconstruction.coid,
      cn_patient_surgery_reconstruction.company_code,
      cn_patient_surgery_reconstruction.recstr_date,
      cn_patient_surgery_reconstruction.surgery_recstr_type_text,
      cn_patient_surgery_reconstruction.declined_ind,
      cn_patient_surgery_reconstruction.hashbite_ssk,
      cn_patient_surgery_reconstruction.source_system_code,
      cn_patient_surgery_reconstruction.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_surgery_reconstruction
  ;
