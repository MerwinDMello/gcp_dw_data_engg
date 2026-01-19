CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_abstraction_measure
   OPTIONS(description='Contains all the measures captured in abstraction')
  AS SELECT
      ref_abstraction_measure.abstraction_measure_sk,
      ref_abstraction_measure.abstraction_measure_name,
      ref_abstraction_measure.source_system_code,
      ref_abstraction_measure.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_abstraction_measure
  ;
