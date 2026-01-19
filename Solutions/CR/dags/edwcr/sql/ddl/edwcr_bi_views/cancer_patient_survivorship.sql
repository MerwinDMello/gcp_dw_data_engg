-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_survivorship.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_survivorship AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cpspt.task_desc_text,
    cpspt.task_closed_date,
    cpspt.comment_text AS task_comment_text,
    cpspt.contact_date AS task_contact_date,
    rcm.contact_method_desc AS task_contact_method_desc,
    cpspt.task_resolution_date,
    cpspt.contact_result_text AS task_contact_result_text,
    rs.status_desc AS task_status_desc,
    cpspt.source_system_code,
    cpspt.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_survivorship_plan_task AS cpspt ON cptd.cn_patient_id = cpspt.nav_patient_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_status AS rs ON cpspt.task_status_id = rs.status_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_contact_method AS rcm ON cpspt.contact_method_id = rcm.contact_method_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  QUALIFY row_number() OVER (PARTITION BY cptd.cancer_patient_tumor_driver_sk, nav_survivorship_plan_task_sid ORDER BY cptd.cancer_patient_tumor_driver_sk DESC) = 1
;
