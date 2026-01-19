-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_medication.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_medication AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cpt.definitive_chemo_date,
    cpt.definitive_immuno_date,
    coalesce(t48.lookup_desc, cnmo.drug_dose_measurement_text) AS drug_dose_measurement_text,
    coalesce(nsc.nsc_desc, cnmo.drug_name) AS drug_name,
    t47.lookup_desc AS drug_route_text,
    coalesce(cpmo.total_drug_dose_amt, CAST(cnmo.drug_dose_amt_text AS Numeric)) AS drug_dose_amt_text,
    coalesce(cpmo.drug_frequency_num, CASt(cnmo.cycle_frequency_text AS Numeric)) AS drug_frequency_text,
    t19.lookup_desc AS chemo_declined_reason_text,
    t20.lookup_desc AS hormone_declined_reason_text,
    t21.lookup_desc AS immuno_declined_reason_text,
    CASE
      WHEN upper(rtt.treatment_type_desc) LIKE '%ENDOCRINE%'
       OR upper(rtt.treatment_type_desc) LIKE '%HORMONAL%' THEN coalesce(cptt.treatment_start_date, cpt.definitive_hormone_date)
      ELSE cpt.definitive_hormone_date
    END AS definitive_hormone_date,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS cptt ON cpt.tumor_id = cptt.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_cr_treatment_type AS rtt ON cptt.treatment_type_id = rtt.treatment_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_medical_oncology AS cpmo ON cptt.treatment_id = cpmo.treatment_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_medical_oncology AS cnmo ON cptd.cn_patient_id = cnmo.nav_patient_id
     AND cptd.cn_tumor_type_id = cnmo.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_national_service_center AS nsc ON cpmo.nsc_id = nsc.nsc_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t47 ON cpmo.drug_route_id = t47.master_lookup_sid
     AND t47.lookup_id = 47
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t48 ON cpmo.drug_dose_unit_id = t48.master_lookup_sid
     AND t48.lookup_id = 48
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t19 ON cpt.chemo_declined_reason_id = t19.master_lookup_sid
     AND t19.lookup_id = 19
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t20 ON cpt.hormone_declined_reason_id = t20.master_lookup_sid
     AND t20.lookup_id = 20
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t21 ON cpt.immuno_declined_reason_id = t21.master_lookup_sid
     AND t21.lookup_id = 21
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cpmo.treatment_id IS NOT NULL
   OR cnmo.nav_patient_id IS NOT NULL
;
