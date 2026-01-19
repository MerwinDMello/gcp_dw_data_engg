CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factvacancies_daily AS SELECT
    fact_vacancy_daily.date_id AS pe_date,
    fact_vacancy_daily.coid,
    fact_vacancy_daily.process_level_code,
    fact_vacancy_daily.process_level_uid AS pl_uid,
    fact_vacancy_daily.dept_num AS cost_center,
    fact_vacancy_daily.rn_group_name AS rn_grouping,
    fact_vacancy_daily.requisition_num,
    fact_vacancy_daily.requisition_sid,
    fact_vacancy_daily.requisition_approval_date,
    fact_vacancy_daily.requisition_closed_date AS closed_date,
    fact_vacancy_daily.key_talent_id,
    fact_vacancy_daily.position_sid,
    fact_vacancy_daily.position_key,
    fact_vacancy_daily.schedule_work_code,
    fact_vacancy_daily.schedule_work_code_desc,
    fact_vacancy_daily.open_fte_percent AS fte_percent,
    fact_vacancy_daily.prn_tier_text AS prn_tier,
    fact_vacancy_daily.workforce_category_text AS workforce_category,
    fact_vacancy_daily.integrated_lob_id,
    CASE
      WHEN fact_vacancy_daily.status_code = '01' THEN 'FT'
      WHEN fact_vacancy_daily.status_code = '02' THEN 'PT'
      WHEN fact_vacancy_daily.status_code = '03' THEN 'PRN'
      WHEN fact_vacancy_daily.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    fact_vacancy_daily.source_system_code,
    fact_vacancy_daily.metric_numerator_qty AS vacancies,
    fact_vacancy_daily.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_vacancy_daily
;
