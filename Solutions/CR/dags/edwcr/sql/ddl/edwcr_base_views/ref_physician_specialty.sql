CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_physician_specialty
   OPTIONS(description='Contains the list of physician specialties.')
  AS SELECT
      ref_physician_specialty.physician_specialty_id,
      ref_physician_specialty.physician_specialty_desc,
      ref_physician_specialty.source_system_code,
      ref_physician_specialty.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_physician_specialty
  ;
