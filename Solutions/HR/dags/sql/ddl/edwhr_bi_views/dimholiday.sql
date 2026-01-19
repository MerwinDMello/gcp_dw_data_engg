CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.dimholiday AS SELECT DISTINCT
    lud.date_id,
    lud.day_of_week,
    lud.dow_desc_s,
    lud.week_end_flag,
    lud.hca_holiday_flag
  FROM
    {{ params.param_pub_views_dataset_name }}.lu_date AS lud
  WHERE lud.date_id BETWEEN '2016-01-01' AND current_date('US/Central')
   AND (lud.week_end_flag = 1
   OR upper(lud.hca_holiday_flag) = '1')
;
