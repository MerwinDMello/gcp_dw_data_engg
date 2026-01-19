/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.microstep_pathway_sla AS SELECT
      a.pathway_id,
      a.microstep_num,
      a.sla_day_cnt,
      a.day_type_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.microstep_pathway_sla AS a
  ;

