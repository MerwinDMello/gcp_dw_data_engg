-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/rad_onc_patient_plan.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.rad_onc_patient_plan AS SELECT
    a.patient_plan_sk,
    a.plan_purpose_sk,
    a.site_sk,
    a.source_patient_plan_id,
    a.patient_course_sk,
    a.plan_status_id,
    a.plan_unique_id_text,
    a.plan_creation_date_time,
    a.treatment_start_date_time,
    a.treatment_end_date_time,
    a.history_user_name,
    a.history_date_time,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_plan AS a
;
