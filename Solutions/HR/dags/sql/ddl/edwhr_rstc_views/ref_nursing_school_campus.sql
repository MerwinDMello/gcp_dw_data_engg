/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.ref_nursing_school_campus AS SELECT
    a.campus_id,
    a.campus_name,
    a.campus_code,
    a.nursing_school_id,
    a.addr_sid,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.ref_nursing_school_campus AS a
;
