
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.ref_action AS SELECT
    a.action_code,
    a.lawson_company_num,
    a.active_flag,
    a.action_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.ref_action AS a
;
