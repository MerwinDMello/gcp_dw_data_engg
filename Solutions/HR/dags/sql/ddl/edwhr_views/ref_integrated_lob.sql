/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.ref_integrated_lob AS SELECT
      a.integrated_lob_id,
      a.category_desc,
      a.sub_category_desc,
      a.process_level_code,
      a.dept_code,
      a.lob_code,
      a.sub_lob_code,
      a.functional_dept_desc,
      a.sub_functional_dept_desc,
      a.match_level_num,
      a.match_level_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.ref_integrated_lob AS a
  ;

