CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.sys_calendar AS
SELECT
  calendar_date,
  day_of_week,
  day_of_month,
  day_of_year,
  day_of_calendar,
  weekday_of_month,
  week_of_month,
  week_of_year,
  week_of_calendar,
  month_of_quarter,
  month_of_year,
  month_of_calendar,
  quarter_of_year,
  quarter_of_calendar,
  year_of_calendar
FROM
  {{ params.param_cr_core_dataset_name }}.sys_calendar
;