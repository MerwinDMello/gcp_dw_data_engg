/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_pathway AS SELECT
      a.pathway_id,
      a.pathway_num,
      a.subpathway_code,
      a.pathway_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_pathway AS a
  ;

