-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_treatment.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_treatment AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cr.treatment_comment_text,
    coalesce(cr.treatment_performing_physician_name, cpd.physician_name) AS treatment_performing_physician_name,
    cr.treatment_type_desc,
    cr.treatment_type_group_code,
    cr.treatment_type_group_desc,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN (
      SELECT
          cpt.cr_patient_id,
          cpt.tumor_primary_site_id,
          cptt.tumor_id,
          cptt.comment_text AS treatment_comment_text,
          concat(cpc.contact_last_name, ', ', cpc.contact_first_name) AS treatment_performing_physician_name,
          cptt.treatment_performing_physician_code,
          rtt.treatment_type_desc,
          rttg.treatment_type_group_code,
          rttg.treatment_type_group_desc
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS cptt ON cpt.tumor_id = cptt.tumor_id
          LEFT OUTER JOIN (
            SELECT DISTINCT
                cpc1.contact_num_code,
                cpc1.contact_last_name,
                cpc1.contact_first_name
              FROM
                {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact AS cpc1
                INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS rlc ON cpc1.contact_type_id = rlc.master_lookup_sid
                 AND rlc.lookup_id = 10
                 AND upper(rlc.lookup_code) = 'D'
          ) AS cpc ON cptt.treatment_performing_physician_code = cpc.contact_num_code
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type AS rtt ON cptt.treatment_type_id = rtt.treatment_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group AS rttg ON cptt.treatment_type_group_id = rttg.treatment_type_group_id
    ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure AS cpt1 ON cptd.cn_patient_id = cpt1.nav_patient_id
     AND cptd.cn_tumor_type_id = cpt1.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS cpd ON cpt1.med_spcl_physician_id = cpd.physician_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cr.tumor_id IS NOT NULL
   OR cpt1.tumor_type_id IS NOT NULL
;
