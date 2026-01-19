-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_driver.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_driver AS SELECT
    a.cancer_patient_tumor_driver_sk,
    a.cancer_patient_driver_sk,
    a.cancer_tumor_driver_sk,
    a.coid,
    a.company_code,
    a.cr_patient_id,
    a.cn_patient_id,
    a.cp_patient_id,
    a.cr_tumor_primary_site_id,
    a.cn_tumor_type_id,
    a.cp_icd_oncology_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver AS a
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
