CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_lung_lobe_location
   OPTIONS(description='Contains the different lung lobe locations in a distinct list.')
  AS SELECT
      ref_lung_lobe_location.lung_lobe_location_id,
      ref_lung_lobe_location.lung_lobe_location_desc,
      ref_lung_lobe_location.source_system_code,
      ref_lung_lobe_location.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_lung_lobe_location
  ;
