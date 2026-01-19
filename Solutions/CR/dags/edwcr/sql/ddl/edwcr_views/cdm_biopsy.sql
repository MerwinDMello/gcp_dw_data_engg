-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_biopsy.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_biopsy AS SELECT
    a.procedure_sk,
    a.procedure_text,
    a.patient_dw_id,
    a.coid,
    a.company_code,
    a.biopsy_ts,
    a.biopsy_performing_physician_name,
    a.physician_specialty_name,
    a.role_plyr_sk,
    a.physician_npi,
    a.priority_sequence_num,
    a.anesthesia_code_sk,
    a.anesthesia_code_desc,
    a.encounter_sk,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_biopsy AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
