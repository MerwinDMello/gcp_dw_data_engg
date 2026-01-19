-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_rad_onc_patient_diagnosis.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_rad_onc_patient_diagnosis AS SELECT
    a.fact_patient_diagnosis_sk,
    a.diagnosis_code_sk,
    a.patient_sk,
    a.diagnosis_status_id,
    a.cell_category_id,
    a.cell_grade_id,
    a.laterality_id,
    a.stage_id,
    a.stage_status_id,
    a.recurrence_id,
    a.invasion_id,
    a.confirmed_diagnosis_id,
    a.diagnosis_type_id,
    a.site_sk,
    a.source_fact_patient_diagnosis_id,
    a.diagnosis_status_date,
    a.diagnosis_text,
    a.clinical_text,
    a.pathology_comment_text,
    a.node_num,
    a.positive_node_num,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient_diagnosis AS a
;
