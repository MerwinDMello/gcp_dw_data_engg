-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/fact_rad_onc_patient_toxicity.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.fact_rad_onc_patient_toxicity AS SELECT
    a.fact_patient_toxicity_sk,
    a.toxicity_component_sk,
    a.toxicity_assessment_type_sk,
    a.activity_transaction_sk,
    a.patient_sk,
    a.scheme_id,
    a.toxicity_cause_certainty_type_id,
    a.toxicity_cause_type_id,
    a.diagnosis_code_sk,
    a.site_sk,
    a.source_fact_patient_toxicity_id,
    a.assessment_date_time,
    a.toxicity_effective_date,
    a.toxicity_grade_num,
    a.valid_entry_ind,
    a.toxicity_approved_date_time,
    a.assessment_performed_date_time,
    a.toxicity_reason_text,
    a.toxicity_approved_ind,
    a.toxicity_header_valid_entry_ind,
    a.revision_num,
    a.log_id,
    a.run_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.fact_rad_onc_patient_toxicity AS a
;
