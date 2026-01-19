-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_procedure.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_procedure AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    rct.core_record_type_desc AS procedure_core_record_type_desc,
    coalesce(cptt.treatment_start_date, cpp.procedure_date) AS procedure_date,
    cpp.palliative_ind AS procedure_palliative_ind,
    cpd.physician_name AS procedure_performing_physician_name,
    rpt.procedure_type_desc,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure AS cpp ON cptd.cn_patient_id = cpp.nav_patient_id
     AND cptd.cn_tumor_type_id = cpp.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_procedure_type AS rpt ON cpp.procedure_type_id = rpt.procedure_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cpp.med_spcl_physician_id = cpd.physician_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_treatment AS cptt ON cpt.tumor_id = cptt.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_core_record_type AS rct ON cpp.core_record_type_id = rct.core_record_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cpp.nav_patient_id IS NOT NULL
   OR cpt.cr_patient_id IS NOT NULL
;
