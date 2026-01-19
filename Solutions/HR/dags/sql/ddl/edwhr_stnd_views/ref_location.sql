
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.ref_location AS SELECT
    a.location_code,
    a.location_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.ref_location AS a
;
