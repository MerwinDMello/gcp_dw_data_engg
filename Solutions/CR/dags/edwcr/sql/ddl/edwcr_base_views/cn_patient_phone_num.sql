CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_phone_num
   OPTIONS(description='Contains the different phone numbers a patient could be associated with.')
  AS SELECT
      cn_patient_phone_num.nav_patient_id,
      cn_patient_phone_num.phone_num_type_code,
      cn_patient_phone_num.phone_num,
      cn_patient_phone_num.source_system_code,
      cn_patient_phone_num.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_phone_num
  ;
