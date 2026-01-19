CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.xrefreshdatetime AS SELECT
    'HR Data' AS data_set,
    max(hrmet.dw_last_update_date_time) AS update_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
;
