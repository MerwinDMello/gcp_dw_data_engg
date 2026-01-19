-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_lab_result.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_lab_result AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cplr.comment_text AS lab_comment_text,
    rcrt.core_record_type_desc AS lab_core_record_type_desc,
    cplr.lab_date,
    cplr.lab_result_text,
    rlt.lab_type_desc,
    cplr.source_system_code,
    cplr.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_lab_result AS cplr ON cptd.cn_patient_id = cplr.nav_patient_id
     AND cptd.cn_tumor_type_id = cplr.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_core_record_type AS rcrt ON cplr.core_record_type_id = rcrt.core_record_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_lab_type AS rlt ON cplr.lab_type_id = rlt.lab_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  QUALIFY row_number() OVER (PARTITION BY cptd.cancer_patient_tumor_driver_sk, nav_patient_lab_result_sid ORDER BY cptd.cancer_patient_tumor_driver_sk DESC) = 1
;
