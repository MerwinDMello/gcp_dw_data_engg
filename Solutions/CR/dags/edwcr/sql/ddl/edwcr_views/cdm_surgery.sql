-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_surgery.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_surgery AS SELECT
    a.patient_dw_id,
    a.surgical_case_seq,
    a.activity_date,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.primary_surgeon_dw_id,
    a.surgeon_npi,
    a.physician_name,
    a.surgeon_start_date_id,
    a.surgeon_start_time,
    a.surgeon_end_date_id,
    a.surgeon_end_time,
    a.procedure_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_surgery AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
