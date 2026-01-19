CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail
   OPTIONS(description='The details for each physician.')
  AS SELECT
      cn_physician_detail.physician_id,
      cn_physician_detail.physician_name,
      cn_physician_detail.physician_phone_num,
      cn_physician_detail.source_system_code,
      cn_physician_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_physician_detail
  ;
