-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cancer_patient_id_abstraction_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cancer_patient_id_abstraction_detail AS SELECT
    a.cancer_abstraction_sk,
    a.abstraction_measure_sk,
    a.cancer_patient_id_output_sk,
    a.message_control_id_text,
    a.coid,
    a.company_code,
    a.patient_dw_id,
    a.pat_acct_num,
    a.predicted_value_text,
    a.submitted_value_text,
    a.suggested_value_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_abstraction_detail AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
