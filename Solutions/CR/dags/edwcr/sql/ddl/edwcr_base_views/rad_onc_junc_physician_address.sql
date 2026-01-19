CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_junc_physician_address
   OPTIONS(description='This table will be a junction for physician address')
  AS SELECT
      rad_onc_junc_physician_address.physician_sk,
      rad_onc_junc_physician_address.address_type_code,
      rad_onc_junc_physician_address.address_sk,
      rad_onc_junc_physician_address.primary_address_ind,
      rad_onc_junc_physician_address.source_system_code,
      rad_onc_junc_physician_address.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_junc_physician_address
  ;
