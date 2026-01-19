-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_navigation.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_navigation AS SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    rcm.contact_method_desc,
    cpc.comment_text AS communication_comment_text,
    cpc.contact_date,
    cpc.other_person_contacted_text,
    cpc.other_purpose_detail_text,
    rcp.contact_purpose_desc,
    cpcd.contact_detail_measure_value_text AS contact_detail_purpose_reason_text,
    cpc.time_spent_amount_text,
    cpt.electronic_folder_id_text,
    cpt.identification_period_text,
    cp.nav_create_date,
    rs.status_desc AS nav_status_desc,
    cpt.referral_date,
    rf.facility_name AS referral_source_facility_name,
    navigator_name,
    navigator_3_4_id,
    rtt.tumor_type_desc AS tumor_site_desc,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cpt ON cptd.cn_patient_id = cpt.nav_patient_id
     AND cptd.cn_tumor_type_id = cpt.tumor_type_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient AS cp ON cpt.nav_patient_id = cp.nav_patient_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_contact AS cpc ON cpt.nav_patient_id = cpc.nav_patient_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_navigator AS rn ON cpt.navigator_id = rn.navigator_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_tumor_type AS rtt ON cpt.tumor_type_id = rtt.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_contact_purpose AS rcp ON cpc.contact_purpose_id = rcp.contact_purpose_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_contact_method AS rcm ON cpc.contact_method_id = rcm.contact_method_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_status AS rs ON cpt.nav_status_id = rs.status_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_contact_detail AS cpcd ON cpc.cn_patient_contact_sid = cpcd.cn_patient_contact_sid
     AND cpcd.contact_detail_measure_type_id = 48
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_facility AS rf ON cpt.referral_source_facility_id = rf.facility_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  QUALIFY row_number() OVER (PARTITION BY cptd.cancer_patient_tumor_driver_sk, cpc.cn_patient_contact_sid ORDER BY cptd.cancer_patient_tumor_driver_sk DESC) = 1
;
