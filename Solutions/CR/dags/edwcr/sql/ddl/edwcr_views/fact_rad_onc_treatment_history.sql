-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_rad_onc_treatment_history.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_rad_onc_treatment_history AS SELECT
    a.fact_treatment_history_sk,
    a.patient_course_sk,
    a.patient_plan_sk,
    a.patient_sk,
    a.treatment_intent_type_id,
    a.clinical_status_id,
    a.plan_status_id,
    a.field_technique_id,
    a.technique_id,
    a.technique_label_id,
    a.technique_delivery_type_id,
    a.site_sk,
    a.source_fact_treatment_history_id,
    a.completion_date_time,
    a.first_treatment_date_time,
    a.last_treatment_date_time,
    a.status_date_time,
    a.active_ind,
    a.planned_dose_rate_num,
    a.course_dose_delivered_amt,
    a.course_dose_planned_amt,
    a.course_dose_remaining_amt,
    a.other_course_dose_delivered_amt,
    a.dose_correction_amt,
    a.total_dose_limit_amt,
    a.daily_dose_limit_amt,
    a.session_dose_limit_amt,
    a.primary_ind,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_treatment_history AS a
;
