CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_phone
   OPTIONS(description='Contains the different phone numbers a patient , contact or doctor could be associated with.')
  AS SELECT
      rad_onc_phone.phone_num_sk,
      rad_onc_phone.phone_num_text,
      rad_onc_phone.source_system_code,
      rad_onc_phone.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_phone
  ;
