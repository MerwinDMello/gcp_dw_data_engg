-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_imaging.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_imaging AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    rcrt.core_record_type_desc,
    rs.side_desc AS imaging_area_side_desc,
    cpi.birad_scale_code,
    cpi.comment_text AS imaging_comment_text,
    coalesce(cpro.radiation_treatment_start_date, cpi.imaging_date) AS imaging_date,
    rf.facility_name AS imaging_facility_name,
    rim.imaging_mode_desc,
    rit.imaging_type_desc,
    cpi.imaging_location_text,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_imaging AS cpi ON cptd.cn_patient_id = cpi.nav_patient_id
     AND cptd.cn_tumor_type_id = cpi.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_imaging_type AS rit ON cpi.imaging_type_id = rit.imaging_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cpi.imaging_facility_id = rf.facility_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_imaging_mode AS rim ON cpi.imaging_mode_id = rim.imaging_mode_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs ON cpi.imaging_area_side_id = rs.side_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpi.core_record_type_id = rcrt.core_record_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_treatment AS crpt ON cpt.tumor_id = crpt.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cr_patient_radiation_oncology AS cpro ON crpt.treatment_id = cpro.treatment_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cpro.treatment_id IS NOT NULL
   OR cpi.nav_patient_id IS NOT NULL
;
