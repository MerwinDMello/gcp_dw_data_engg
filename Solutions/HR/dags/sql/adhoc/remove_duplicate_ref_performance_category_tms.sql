DELETE
FROM `{{ params.param_hr_core_dataset_name }}.ref_performance_category`
WHERE CAST(dw_last_update_date_time AS date) <> DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
AND performance_category_desc NOT IN ('Customer Focus', 'GrowthLink');