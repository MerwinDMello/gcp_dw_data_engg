CREATE OR REPLACE FUNCTION `{{ params.param_hr_bi_views_dataset_name }}.months_between_impl`(date_1 DATE, date_2 DATE) AS (
CASE
      WHEN date_1 = date_2 
        THEN 0
      WHEN EXTRACT(DAY FROM DATE_ADD(date_1, INTERVAL 1 DAY)) = 1
        AND EXTRACT(DAY FROM DATE_ADD(date_2, INTERVAL 1 DAY)) = 1 
        THEN DATE_DIFF(date_1,date_2, MONTH)
      WHEN EXTRACT(DAY FROM date_1) = 1 
        AND EXTRACT(DAY FROM DATE_ADD(date_2, INTERVAL 1 DAY)) = 1 
        THEN DATE_DIFF(DATE_ADD(date_1, INTERVAL -1 DAY), date_2, MONTH) + 1/31
      ELSE DATE_DIFF(DATE_ADD(date_1, INTERVAL -1 DAY), date_2, MONTH) - 1 + EXTRACT(DAY FROM DATE_ADD(date_1, INTERVAL -1 DAY)) / 31 + (31 - EXTRACT(DAY FROM date_2) + 1) / 31
    END
);
