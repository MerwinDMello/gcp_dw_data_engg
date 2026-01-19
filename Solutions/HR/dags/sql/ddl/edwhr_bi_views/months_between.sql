  CREATE OR REPLACE FUNCTION `{{ params.param_hr_bi_views_dataset_name }}.months_between` (date_1 DATE, date_2 DATE) AS (
--TRUNC(`hca-hin-dev-cur-hr.edwhr_bi_views_copy.months_between_impl`(date_1, date_2),9)
  cast(round(`{{ params.param_hr_bi_views_dataset_name }}.months_between_impl`(date_1, date_2), 5) as int64)
  --cast(`hca-hin-dev-cur-hr.edwhr_bi_views_copy.months_between_impl`(date_1, date_2) as int64)
);
