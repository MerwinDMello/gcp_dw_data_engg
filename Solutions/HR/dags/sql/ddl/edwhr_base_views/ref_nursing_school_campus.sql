CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.ref_nursing_school_campus AS SELECT
    ref_nursing_school_campus.campus_id,
    ref_nursing_school_campus.campus_name,
    ref_nursing_school_campus.campus_code,
    ref_nursing_school_campus.nursing_school_id,
    ref_nursing_school_campus.addr_sid,
    ref_nursing_school_campus.source_system_code,
    ref_nursing_school_campus.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.ref_nursing_school_campus
;