CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_imaging_mode
   OPTIONS(description='Contains a distinct list of different imaging modes.')
  AS SELECT
      ref_imaging_mode.imaging_mode_id,
      ref_imaging_mode.imaging_mode_desc,
      ref_imaging_mode.source_system_code,
      ref_imaging_mode.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_imaging_mode
  ;
