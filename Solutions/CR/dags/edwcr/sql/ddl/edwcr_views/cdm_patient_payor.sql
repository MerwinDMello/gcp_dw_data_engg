-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/{{ params.param_cr_views_dataset_name }}/cdm_patient_payor.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_views_dataset_name }}.cdm_patient_payor AS SELECT
    a.patient_dw_id,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.payor_dw_id_ins1,
    a.payor_name,
    a.iplan_id_ins1,
    a.plan_desc,
    a.major_payor_id_ins1,
    a.major_payor_group_desc,
    a.financial_class_code,
    a.financial_class_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cdm_patient_payor AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
