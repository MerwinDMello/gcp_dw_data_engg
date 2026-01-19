CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimstatus AS SELECT DISTINCT
    stas.status_code AS emp_status_code
  FROM
    {{ params.param_hr_base_views_dataset_name }}.status AS stas
  WHERE date(stas.valid_to_date) = date'9999-12-31'
   AND stas.status_sid IN(
    SELECT DISTINCT
        hrmet.auxiliary_status_sid
      FROM
        {{ params.param_hr_base_views_dataset_name }}.fact_hr_metric_snapshot AS hrmet
  )
;
