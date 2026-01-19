-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cn_patient_heme_treatment_regimen.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cn_patient_heme_treatment_regimen AS SELECT
    a.cn_patient_heme_diagnosis_sid,
    a.nav_patient_id,
    a.regimen_id,
    a.treatment_phase_id,
    a.pathway_var_reason_id,
    a.tumor_type_id,
    a.diagnosis_result_id,
    a.nav_diagnosis_id,
    a.navigator_id,
    a.coid,
    a.company_code,
    a.planned_start_date,
    a.actual_start_date,
    a.drug_text,
    a.cycle_num,
    a.cycle_length_num,
    a.cycle_frequency_text,
    a.ordinal_cycle_num,
    a.pathway_ind,
    a.pathway_text,
    a.pathway_compliant_ind,
    a.treatment_plan_document_date,
    a.prior_plan_document_timeframe_ind,
    a.treatment_regimen_comment_text,
    a.hashbite_ssk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_treatment_regimen AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
