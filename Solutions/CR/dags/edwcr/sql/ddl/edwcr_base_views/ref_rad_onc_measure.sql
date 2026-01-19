CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_measure
   OPTIONS(description='Contains all the measures captured in radiation oncology')
  AS SELECT
      ref_rad_onc_measure.rad_onc_measure_id,
      ref_rad_onc_measure.rad_onc_measure_type,
      ref_rad_onc_measure.rad_onc_measure_name,
      ref_rad_onc_measure.source_system_code,
      ref_rad_onc_measure.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_measure
  ;
