CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_status
   OPTIONS(description='Contains a distinct list of statuses for various entities.')
  AS SELECT
      ref_status.status_id,
      ref_status.status_type_desc,
      ref_status.status_desc,
      ref_status.source_system_code,
      ref_status.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_status
  ;
