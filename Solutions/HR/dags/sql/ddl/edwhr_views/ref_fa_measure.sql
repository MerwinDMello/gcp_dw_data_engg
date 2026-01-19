/***************************************************************************************
   B A S E   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_fa_measure AS SELECT
      ref_fa_measure.fa_measure_code,
      ref_fa_measure.fa_measure_desc,
      ref_fa_measure.source_system_code,
      ref_fa_measure.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_fa_measure
  ;

