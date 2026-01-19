CREATE OR REPLACE VIEW {{ params.param_hr_bi_views_dataset_name }}.factvacancies AS SELECT
    fact_vacancy_snapshot.period_end_date AS pe_date,
    fact_vacancy_snapshot.coid,
    fact_vacancy_snapshot.process_level_code,
    fact_vacancy_snapshot.process_level_uid AS pl_uid,
    fact_vacancy_snapshot.dept_num AS cost_center,
    fact_vacancy_snapshot.rn_group_name AS rn_grouping,
    fact_vacancy_snapshot.requisition_num,
    fact_vacancy_snapshot.requisition_sid,
    fact_vacancy_snapshot.requisition_approval_date,
    fact_vacancy_snapshot.requisition_closed_date AS closed_date,
    fact_vacancy_snapshot.key_talent_id,
    fact_vacancy_snapshot.position_sid,
    fact_vacancy_snapshot.position_key,
    fact_vacancy_snapshot.schedule_work_code,
    fact_vacancy_snapshot.schedule_work_code_desc,
    fact_vacancy_snapshot.open_fte_percent AS fte_percent,
    fact_vacancy_snapshot.prn_tier_text AS prn_tier,
    fact_vacancy_snapshot.workforce_category_text AS workforce_category,
    fact_vacancy_snapshot.integrated_lob_id,
    CASE
      WHEN fact_vacancy_snapshot.status_code = '01' THEN 'FT'
      WHEN fact_vacancy_snapshot.status_code = '02' THEN 'PT'
      WHEN fact_vacancy_snapshot.status_code = '03' THEN 'PRN'
      WHEN fact_vacancy_snapshot.status_code = '04' THEN 'TEMP'
      ELSE CAST(NULL as STRING)
    END AS emp_status,
    fact_vacancy_snapshot.source_system_code,
    fact_vacancy_snapshot.metric_numerator_qty AS vacancies
  FROM
    {{ params.param_hr_base_views_dataset_name }}.fact_vacancy_snapshot
;
