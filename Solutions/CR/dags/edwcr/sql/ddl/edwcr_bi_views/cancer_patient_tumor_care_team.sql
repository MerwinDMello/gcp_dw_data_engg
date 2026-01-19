-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_care_team.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_care_team AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cpc.consult_date,
    cpc.consult_notes_text,
    cpc.consult_other_type_text,
    cpc.consult_phone_num,
    cpd2.physician_name AS consult_physician_name,
    rct.consult_type_desc,
    concat(crt1.contact_last_name, ', ', crt1.contact_first_name) AS managing_physician_name,
    cpd.physician_name AS gynecologist_physician_name,
    cpd1.physician_name AS primary_care_physician_name,
    rrp.referring_physician_name,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpt ON cptd.cn_patient_id = cpt.nav_patient_id
     AND cptd.cn_tumor_type_id = cpt.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cp ON cpt.nav_patient_id = cp.nav_patient_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_consultation AS cpc ON cpt.nav_patient_id = cpc.nav_patient_id
     AND cpt.tumor_type_id = cpc.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cp.gynecologist_physician_id = cpd.physician_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd1 ON cp.primary_care_physician_id = cpd1.physician_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd2 ON cpc.med_spcl_physician_id = cpd2.physician_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_consult_type AS rct ON cpc.consult_type_id = rct.consult_type_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician AS rrp ON cpt.referring_physician_id = rrp.referring_physician_id
    LEFT OUTER JOIN (
      SELECT
          crt.cr_patient_id,
          crt.tumor_primary_site_id,
          cr1.contact_first_name,
          cr1.contact_last_name
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS crt
          INNER JOIN (
            SELECT DISTINCT
                crtt1.managing_physician_code,
                cpc_0.contact_first_name,
                cpc_0.contact_last_name
              FROM
                {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS crtt1
                INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact AS cpc_0 ON crtt1.managing_physician_code = cpc_0.contact_num_code
                INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS rlc ON cpc_0.contact_type_id = rlc.master_lookup_sid
                 AND rlc.lookup_id = 10
                 AND upper(rlc.lookup_code) = 'D'
          ) AS cr1 ON crt.managing_physician_code = cr1.managing_physician_code
    ) AS crt1 ON cptd.cr_patient_id = crt1.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = crt1.tumor_primary_site_id
  WHERE crt1.cr_patient_id IS NOT NULL
   OR cpt.nav_patient_id IS NOT NULL
;
