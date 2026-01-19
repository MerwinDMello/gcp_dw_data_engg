CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_phone_num
   OPTIONS(description='Contains the different phone numbers a patient could be associated with.')
  AS SELECT
      cr_patient_phone_num.cr_patient_id,
      cr_patient_phone_num.patient_contact_id,
      cr_patient_phone_num.phone_num_type_code,
      cr_patient_phone_num.phone_num,
      cr_patient_phone_num.source_system_code,
      cr_patient_phone_num.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_phone_num
  ;
