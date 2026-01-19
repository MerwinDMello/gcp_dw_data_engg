-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_radiation_oncology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_radiation_oncology AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cr.definitive_radiation_treatment_end_date AS definitive_radiation_end_date,
    cr.definitive_radiation_date AS definitive_radiation_start_date,
    cr.definitive_radiation_hospital_text AS definitive_radiation_facility_text,
    cr.definitive_chemo_summary_text,
    cr.definitive_diagnostic_stage_summary_text,
    cr.definitive_hormone_summary_text,
    cr.definitive_immuno_summary_text,
    cn.comment_text AS comment_text,
    cn.core_record_type_desc,
    cn.elapse_ind,
    cn.elapse_end_date,
    cn.radiation_oncology_reason_text AS elapse_reason_text,
    cn.elapse_start_date,
    cn.treatment_fractions_num,
    cn.site_location_desc AS site_physical_location_desc,
    cn.lung_lobe_location_desc,
    cn.treatment_therapy_schedule_code,
    cn.palliative_ind,
    cr.radiation_performing_physician_name,
    cn.radiation_oncology_physician_name,
    coalesce(cr.radiation_treatment_start_date, cn.treatment_start_date) AS radiation_treatment_start_date,
    coalesce(cr.hospital_name, cn.radiation_oncology_facility_name) AS radiation_treatment_facility_name,
    coalesce(cr.radiation_treatment_end_date, cn.treatment_end_date) AS radiation_treatment_end_date,
    coalesce(cr.radiation_type_desc, cn.radiation_oncology_treatment_type_desc) AS radiation_type_desc,
    cr.radiation_declined_reason_text,
    cr.radiation_boost_modality_text,
    cr.radiation_site_desc AS definitive_radiation_location_text,
    cr.definitive_radiation_volume_summary_text,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN (
      SELECT
          crpt.cr_patient_id,
          crpt.tumor_primary_site_id,
          crpt.definitive_radiation_treatment_end_date,
          crpt.definitive_radiation_date,
          t51.lookup_desc AS definitive_radiation_hospital_text,
          t30.lookup_desc AS definitive_radiation_modality_text,
          t38.lookup_desc AS definitive_chemo_summary_text,
          t37.lookup_desc AS definitive_diagnostic_stage_summary_text,
          t36.lookup_desc AS definitive_hormone_summary_text,
          t35.lookup_desc AS definitive_immuno_summary_text,
          concat(cpc.contact_last_name, ', ', cpc.contact_first_name) AS radiation_performing_physician_name,
          cpro.radiation_treatment_start_date,
          rh.hospital_name,
          cpro.radiation_treatment_end_date,
          t15.lookup_desc AS radiation_type_desc,
          t22.lookup_desc AS radiation_declined_reason_text,
          t32.lookup_desc AS radiation_boost_modality_text,
          t31.lookup_desc AS radiation_site_desc,
          --  RTLocation
          t29.lookup_desc AS definitive_radiation_volume_summary_text
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS crpt
          LEFT OUTER JOIN (
            SELECT
                treatment_id,
                tumor_id,
                treatment_performing_physician_code
              FROM
                {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crptx
                INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group AS rttg ON crptx.treatment_type_group_id = rttg.treatment_type_group_id
                 AND upper(rttg.treatment_type_group_code) = 'R'
          ) AS crpt1 ON crpt.tumor_id = crpt1.tumor_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_radiation_oncology AS cpro ON crpt1.treatment_id = cpro.treatment_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t15 ON cpro.radiation_type_id = t15.master_lookup_sid
           AND t15.lookup_id = 15
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t22 ON crpt.radiation_declined_reason_id = t22.master_lookup_sid
           AND t22.lookup_id = 22
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t29 ON crpt.definitive_radiation_volume_summary_id = t29.master_lookup_sid
           AND t29.lookup_id = 29
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t30 ON crpt.definitive_radiation_modality_id = t30.master_lookup_sid
           AND t30.lookup_id = 30
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t31 ON crpt.radiation_site_id = t31.master_lookup_sid
           AND t31.lookup_id = 31
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t32 ON crpt.radiation_boost_modality_id = t32.master_lookup_sid
           AND t32.lookup_id = 32
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t35 ON crpt.definitive_immuno_summary_id = t35.master_lookup_sid
           AND t35.lookup_id = 35
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t36 ON crpt.definitive_hormone_summary_id = t36.master_lookup_sid
           AND t36.lookup_id = 36
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t37 ON crpt.definitive_diagnostic_stage_summary_id = t37.master_lookup_sid
           AND t37.lookup_id = 37
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t38 ON crpt.definitive_chemo_summary_id = t38.master_lookup_sid
           AND t38.lookup_id = 38
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t51 ON crpt.radiation_hospital_id = t51.master_lookup_sid
           AND t51.lookup_id = 51
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_hospital AS rh ON cpro.radiation_hospital_id = rh.hospital_id
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
          ) AS cpc ON crpt1.treatment_performing_physician_code = cpc.contact_num_code
    ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
    LEFT OUTER JOIN (
      SELECT
          cn_0.nav_patient_id,
          cn_0.tumor_type_id,
          cpro.comment_text,
          rcrt.core_record_type_desc,
          cpro.elapse_ind,
          cpro.elapse_end_date,
          cpro.radiation_oncology_reason_text,
          cpro.elapse_start_date,
          rf.facility_name AS radiation_oncology_facility_name,
          cpro.treatment_fractions_num,
          rsl.site_location_desc,
          rlll.lung_lobe_location_desc,
          cpro.treatment_therapy_schedule_code,
          cpro.palliative_ind,
          cpd.physician_name AS radiation_oncology_physician_name,
          cpro.treatment_start_date,
          cpro.treatment_end_date,
          rtt.treatment_type_desc AS radiation_oncology_treatment_type_desc
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_tumor AS cn_0
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_radiation_oncology AS cpro ON cn_0.nav_patient_id = cpro.nav_patient_id
           AND cn_0.tumor_type_id = cpro.tumor_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type AS rtt ON cpro.treatment_type_id = rtt.treatment_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpro.core_record_type_id = rcrt.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lung_lobe_location AS rlll ON cpro.lung_lobe_location_id = rlll.lung_lobe_location_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cpro.radiation_oncology_facility_id = rf.facility_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cpro.med_spcl_physician_id = cpd.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_site_location AS rsl ON cpro.treatment_site_location_id = rsl.site_location_id
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cr.cr_patient_id IS NOT NULL
   OR cn.nav_patient_id IS NOT NULL
;
