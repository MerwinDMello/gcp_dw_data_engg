-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_discharge.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_discharge AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cpt.nav_end_reason_text,
    rf.facility_name AS treatment_end_facility_name,
    cpd.physician_name AS treatment_end_physician_name,
    cpt.treatment_end_reason_text,
    cpt.source_system_code,
    cpt.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpt ON cptd.cn_patient_id = cpt.nav_patient_id
     AND cptd.cn_tumor_type_id = cpt.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf ON cpt.treatment_end_facility_id = rf.facility_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS cpd ON cpt.treatment_end_physician_id = cpd.physician_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  QUALIFY row_number() OVER (PARTITION BY cptd.cancer_patient_tumor_driver_sk, cn_patient_tumor_sid ORDER BY cptd.cancer_patient_tumor_driver_sk DESC) = 1
;
