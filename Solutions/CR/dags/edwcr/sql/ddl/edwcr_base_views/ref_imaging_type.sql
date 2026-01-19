CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_imaging_type
   OPTIONS(description='Contains a distinct list of imaging types.')
  AS SELECT
      ref_imaging_type.imaging_type_id,
      ref_imaging_type.imaging_type_desc,
      ref_imaging_type.source_system_code,
      ref_imaging_type.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_imaging_type
  ;
