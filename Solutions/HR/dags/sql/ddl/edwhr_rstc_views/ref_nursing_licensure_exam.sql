/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_nursing_licensure_exam AS SELECT
    a.exam_id,
    a.exam_name,
    a.exam_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_nursing_licensure_exam AS a
;
