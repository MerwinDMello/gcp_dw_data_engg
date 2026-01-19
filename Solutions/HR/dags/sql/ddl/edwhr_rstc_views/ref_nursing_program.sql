/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_nursing_program AS SELECT
    a.nursing_program_id,
    a.program_name,
    a.program_type_code,
    a.program_degree_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_nursing_program AS a
;
