CREATE OR REPLACE TABLE {{ params.param_cr_core_dataset_name }}.sys_calendar
(
  calendar_date DATE NOT NULL,
  day_of_week INT64 NOT NULL,
  day_of_month INT64 NOT NULL,
  day_of_year INT64 NOT NULL,
  day_of_calendar INT64 NOT NULL,
  weekday_of_month INT64 NOT NULL,
  week_of_month INT64 NOT NULL,
  week_of_year INT64 NOT NULL,
  week_of_calendar INT64 NOT NULL,
  month_of_quarter INT64 NOT NULL,
  month_of_year INT64 NOT NULL,
  month_of_calendar INT64 NOT NULL,
  quarter_of_year INT64 NOT NULL,
  quarter_of_calendar INT64 NOT NULL,
  year_of_calendar INT64 NOT NULL
);