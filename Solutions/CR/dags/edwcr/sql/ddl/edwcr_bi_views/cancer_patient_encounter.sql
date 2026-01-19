-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_encounter.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_encounter AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    crp.accession_num_code,
    crp.company_code AS cr_company_code,
    crp.coid AS cr_coid,
    cnp.company_code AS cn_company_code,
    cnp.coid AS cn_coid,
    cpd.medical_record_num,
    cpd.patient_market_urn_text,
    cnp.network_mnemonic_cs AS cn_network_mnemonic_cs,
    cnp.facility_mnemonic_cs AS cn_facility_mnemonic_cs,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_core_dataset_name }}.cancer_patient_driver AS cpd ON cptd.cancer_patient_driver_sk = cpd.cancer_patient_driver_sk
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient AS crp ON cptd.cr_patient_id = crp.cr_patient_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient AS cnp ON cptd.cn_patient_id = cnp.nav_patient_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
;
