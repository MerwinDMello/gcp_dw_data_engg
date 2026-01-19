CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_junc_physician_phone
   OPTIONS(description='This table will be a junction for physician phone numbers')
  AS SELECT
      rad_onc_junc_physician_phone.physician_sk,
      rad_onc_junc_physician_phone.phone_num_type_code,
      rad_onc_junc_physician_phone.phone_num_sk,
      rad_onc_junc_physician_phone.source_system_code,
      rad_onc_junc_physician_phone.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_junc_physician_phone
  ;
