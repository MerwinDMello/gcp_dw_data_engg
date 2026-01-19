CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_navigation_measure
   OPTIONS(description='Contains all the measures captured in inavigate')
  AS SELECT
      ref_navigation_measure.nav_measure_id,
      ref_navigation_measure.nav_measure_type,
      ref_navigation_measure.nav_measure_name,
      ref_navigation_measure.source_system_code,
      ref_navigation_measure.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_navigation_measure
  ;
