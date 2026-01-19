CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician
   OPTIONS(description='Contains the details of referring physicians')
  AS SELECT
      ref_referring_physician.referring_physician_id,
      ref_referring_physician.referring_physician_name,
      ref_referring_physician.navigation_referred_ind,
      ref_referring_physician.biopsy_referred_ind,
      ref_referring_physician.surgery_referred_ind,
      ref_referring_physician.source_system_code,
      ref_referring_physician.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_referring_physician
  ;
