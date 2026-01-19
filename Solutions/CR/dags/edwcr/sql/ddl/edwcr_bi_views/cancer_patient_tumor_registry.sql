-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_registry.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_registry AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    crt.abstracted_by_text,
    t44.lookup_desc AS class_case_desc,
    t17.lookup_desc AS case_status_desc,
    t45.lookup_desc AS sequence_primary_desc,
    crspl.system_change_status_date,
    crspl.system_user_id_code,
    csu.user_first_name,
    csu.user_last_name,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS crt ON cptd.cr_patient_id = crt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = crt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_system_productivity_log AS crspl ON crt.cr_patient_id = crspl.cr_patient_id
     AND crt.tumor_id = crspl.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_system_user AS csu ON crspl.system_user_id_code = csu.user_id_code
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t17 ON crt.case_status_id = t17.master_lookup_sid
     AND t17.lookup_id = 17
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t44 ON crt.class_case_id = t44.master_lookup_sid
     AND t44.lookup_id = 44
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t45 ON crt.sequence_primary_id = t45.master_lookup_sid
     AND t45.lookup_id = 45
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
;
