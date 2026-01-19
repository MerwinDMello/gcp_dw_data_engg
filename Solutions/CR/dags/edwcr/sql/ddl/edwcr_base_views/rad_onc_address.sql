CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_address
   OPTIONS(description='Contains the address of the patients and there contacts')
  AS SELECT
      rad_onc_address.address_sk,
      rad_onc_address.address_line_1_text,
      rad_onc_address.address_line_2_text,
      rad_onc_address.full_address_text,
      rad_onc_address.address_comment_text,
      rad_onc_address.source_system_code,
      rad_onc_address.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_address
  ;
