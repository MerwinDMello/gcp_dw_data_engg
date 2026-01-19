
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.fact_vacancy_daily AS SELECT
    fact_vacancy_daily.analytics_msr_sid,
    fact_vacancy_daily.position_sid,
    fact_vacancy_daily.requisition_sid,
    fact_vacancy_daily.date_id,
    fact_vacancy_daily.process_level_uid,
    fact_vacancy_daily.process_level_code,
    fact_vacancy_daily.coid,
    fact_vacancy_daily.company_code,
    fact_vacancy_daily.dept_num,
    fact_vacancy_daily.rn_group_name,
    fact_vacancy_daily.requisition_num,
    fact_vacancy_daily.requisition_approval_date,
    fact_vacancy_daily.requisition_closed_date,
    fact_vacancy_daily.key_talent_id,
    fact_vacancy_daily.position_key,
    fact_vacancy_daily.schedule_work_code,
    fact_vacancy_daily.schedule_work_code_desc,
    fact_vacancy_daily.open_fte_percent,
    fact_vacancy_daily.prn_tier_text,
    fact_vacancy_daily.workforce_category_text,
    fact_vacancy_daily.integrated_lob_id,
    fact_vacancy_daily.status_code,
    fact_vacancy_daily.metric_numerator_qty,
    fact_vacancy_daily.source_system_code,
    fact_vacancy_daily.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.fact_vacancy_daily
;
