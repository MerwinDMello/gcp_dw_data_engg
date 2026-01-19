
CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.fact_vacancy_snapshot AS SELECT
    fact_vacancy_snapshot.analytics_msr_sid,
    fact_vacancy_snapshot.position_sid,
    fact_vacancy_snapshot.requisition_sid,
    fact_vacancy_snapshot.date_id,
    fact_vacancy_snapshot.period_end_date,
    fact_vacancy_snapshot.process_level_uid,
    fact_vacancy_snapshot.process_level_code,
    fact_vacancy_snapshot.coid,
    fact_vacancy_snapshot.company_code,
    fact_vacancy_snapshot.dept_num,
    fact_vacancy_snapshot.rn_group_name,
    fact_vacancy_snapshot.requisition_num,
    fact_vacancy_snapshot.requisition_approval_date,
    fact_vacancy_snapshot.requisition_closed_date,
    fact_vacancy_snapshot.key_talent_id,
    fact_vacancy_snapshot.position_key,
    fact_vacancy_snapshot.schedule_work_code,
    fact_vacancy_snapshot.schedule_work_code_desc,
    fact_vacancy_snapshot.open_fte_percent,
    fact_vacancy_snapshot.prn_tier_text,
    fact_vacancy_snapshot.workforce_category_text,
    fact_vacancy_snapshot.integrated_lob_id,
    fact_vacancy_snapshot.status_code,
    fact_vacancy_snapshot.metric_numerator_qty,
    fact_vacancy_snapshot.source_system_code,
    fact_vacancy_snapshot.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.fact_vacancy_snapshot
;
