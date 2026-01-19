-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_staging.sql
-- Translated from: Teradata
-- Translated to: BigQuery

/***************************************************************************************
USER  V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_staging AS SELECT
    a.cancer_patient_tumor_staging_sk,
    a.cancer_patient_tumor_driver_sk,
    a.cancer_patient_driver_sk,
    a.cancer_tumor_driver_sk,
    a.coid,
    a.company_code,
    a.best_cs_summary_desc,
    a.best_cs_tnm_desc,
    a.tumor_size_num_text,
    a.cancer_stage_code,
    a.cancer_stage_class_method_code,
    a.cancer_stage_type_code,
    a.cancer_stage_result_text,
    a.ajcc_stage_desc,
    a.tumor_size_summary_desc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_staging AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
