-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_surgery.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_surgery AS 

WITH cptd_cpt AS (
  SELECT
      row_number() OVER () AS rn,
      cptd.cancer_patient_tumor_driver_sk,
      cptd.cancer_patient_driver_sk,
      cptd.cancer_tumor_driver_sk,
      cptd.coid,
      cptd.company_code,
      cptd.cr_patient_id AS cr_patient_id_1,
      cptd.cn_patient_id,
      cptd.cp_patient_id,
      cptd.cr_tumor_primary_site_id,
      cptd.cn_tumor_type_id,
      cptd.cp_icd_oncology_code,
      cpt.*
    FROM
      {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
      LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd.cr_patient_id = cpt.cr_patient_id
       AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
), cptd_cpt_crt AS (
  SELECT
      cptd_cpt.rn,
      cptd_cpt.cancer_patient_tumor_driver_sk,
      cptd_cpt.cancer_patient_driver_sk,
      cptd_cpt.cancer_tumor_driver_sk,
      cptd_cpt.coid,
      cptd_cpt.company_code,
      cptd_cpt.cr_patient_id,
      cptd_cpt.cn_patient_id,
      cptd_cpt.cp_patient_id,
      cptd_cpt.cr_tumor_primary_site_id,
      cptd_cpt.cn_tumor_type_id,
      cptd_cpt.cp_icd_oncology_code,
      cptd_cpt.source_system_code,
      cptd_cpt.dw_last_update_date_time,
      cptd_cpt.tumor_id,
      cptd_cpt.cr_patient_id_1,
      cptd_cpt.tumor_grade_id,
      cptd_cpt.case_status_id,
      cptd_cpt.tumor_primary_site_id,
      cptd_cpt.definitive_chemo_summary_id,
      cptd_cpt.definitive_diagnostic_stage_summary_id,
      cptd_cpt.definitive_hormone_summary_id,
      cptd_cpt.definitive_immuno_summary_id,
      cptd_cpt.definitive_surgical_margin_summary_id,
      cptd_cpt.definitive_palliative_care_summary_id,
      cptd_cpt.radiation_boost_modality_id,
      cptd_cpt.radiation_hospital_id,
      cptd_cpt.radiation_site_id,
      cptd_cpt.chemo_declined_reason_id,
      cptd_cpt.hormone_declined_reason_id,
      cptd_cpt.immuno_declined_reason_id,
      cptd_cpt.radiation_declined_reason_id,
      cptd_cpt.primary_site_surgery_summary_id,
      cptd_cpt.treatment_therapy_schedule_id,
      cptd_cpt.definitive_radiation_modality_id,
      cptd_cpt.definitive_radiation_volume_summary_id,
      cptd_cpt.regional_lymph_node_summary_id,
      cptd_cpt.surgery_approach_hospital_id,
      cptd_cpt.primary_site_surgery_hospital_id,
      cptd_cpt.definitive_treatment_facility_id,
      cptd_cpt.secondary_treatment_facility_id,
      cptd_cpt.tertiary_treatment_facility_id,
      cptd_cpt.tumor_size_summary_id,
      cptd_cpt.class_case_id,
      cptd_cpt.sequence_primary_id,
      cptd_cpt.best_cs_summary_id,
      cptd_cpt.best_cs_tnm_id,
      cptd_cpt.tumor_size_num_text,
      cptd_cpt.definitive_chemo_date,
      cptd_cpt.definitive_radiation_date,
      cptd_cpt.definitive_immuno_date,
      cptd_cpt.definitive_hormone_date,
      cptd_cpt.definitive_radiation_treatment_end_date,
      cptd_cpt.first_surgery_chemo_elapsed_day_num,
      cptd_cpt.first_surgery_contact_elapsed_day_num,
      cptd_cpt.first_chemo_surgery_elapsed_day_num,
      cptd_cpt.length_to_chemo_day_num,
      cptd_cpt.length_to_hormone_day_num,
      cptd_cpt.length_to_immuno_day_num,
      cptd_cpt.length_to_surgery_day_num,
      cptd_cpt.length_to_radiation_day_num,
      cptd_cpt.length_to_transplant_day_num,
      cptd_cpt.length_to_first_treatment_day_num,
      cptd_cpt.first_surgery_date,
      cptd_cpt.radiation_elapsed_day_num,
      cptd_cpt.definitive_surgery_date,
      cptd_cpt.admission_date,
      cptd_cpt.survival_num,
      cptd_cpt.abstracted_by_text,
      cptd_cpt.managing_physician_code,
      cptd_cpt.medical_oncology_physician_code,
      cptd_cpt.radiation_oncology_physician_code,
      cptd_cpt.primary_surgeon_physician_code,
      cptd_cpt.cs_ss_factor_1_num_code,
      cptd_cpt.cs_ss_factor_2_num_code,
      cptd_cpt.cs_ss_factor_15_num_code,
      cptd_cpt.cs_ss_factor_16_num_code,
      cptd_cpt.cs_ss_factor_22_num_code,
      cptd_cpt.cs_ss_factor_23_num_code,
      cptd_cpt.cs_ss_factor_25_num_code,
      cptd_cpt.mltp_disciplinary_meet_1_present_date,
      cptd_cpt.mltp_disciplinary_meet_1_present_id,
      cptd_cpt.mltp_disciplinary_meet_2_present_date,
      cptd_cpt.mltp_disciplinary_meet_2_present_id,
      cptd_cpt.mltp_disciplinary_meet_3_present_date,
      cptd_cpt.mltp_disciplinary_meet_3_present_id,
      cptd_cpt.source_system_code AS source_system_code_1,
      cptd_cpt.dw_last_update_date_time AS dw_last_update_date_time_1,
      crt.treatment_id,
      crt.tumor_id AS tumor_id_1,
      crt.treatment_hospital_id,
      crt.treatment_type_id,
      crt.surgical_site_id,
      crt.surgical_margin_result_id,
      crt.treatment_type_group_id,
      crt.clinical_trial_start_date,
      crt.treatment_start_date,
      crt.clinical_trial_text,
      crt.comment_text,
      crt.treatment_performing_physician_code,
      crt.source_system_code AS source_system_code_1_0,
      crt.dw_last_update_date_time AS dw_last_update_date_time_1_0
    FROM
      cptd_cpt
      INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crt ON cptd_cpt.tumor_id = crt.tumor_id
    WHERE crt.treatment_type_group_id IN(
      SELECT
          ref_treatment_type_group.treatment_type_group_id
        FROM
          {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group
        WHERE rtrim(ref_treatment_type_group.treatment_type_group_code) = 'S'
    )
), crt AS (
  SELECT
      cptd_cpt.*,
      CAST(NULL as INT64) AS null_0,
      CAST(NULL as INT64) AS null_1,
      CAST(NULL as INT64) AS null_2,
      CAST(NULL as INT64) AS null_3,
      CAST(NULL as INT64) AS null_4,
      CAST(NULL as INT64) AS null_5,
      CAST(NULL as INT64) AS null_6,
      CAST(NULL as DATE) AS null_7,
      CAST(NULL as DATE) AS null_8,
      CAST(NULL as STRING) AS null_9,
      CAST(NULL as STRING) AS null_10,
      CAST(NULL as STRING) AS null_11,
      CAST(NULL as STRING) AS null_12,
      CAST(NULL as DATETIME) AS null_13
    FROM
      cptd_cpt
    WHERE cptd_cpt.rn NOT IN(
      SELECT
          cptd_cpt_crt.rn AS rn
        FROM
          cptd_cpt_crt
    )
)
SELECT
    cptd_cpt_crt_crt.cancer_patient_tumor_driver_sk,
    cptd_cpt_crt_crt.cancer_patient_driver_sk,
    cptd_cpt_crt_crt.cancer_tumor_driver_sk,
    cptd_cpt_crt_crt.coid,
    cptd_cpt_crt_crt.company_code,
    t28.lookup_desc AS regional_lymph_node_summary_text,
    CASE
      WHEN upper(t18.lookup_code) LIKE 'C50%'
       AND upper(rtrim(t49.lookup_code)) >= '19'
       AND CAST(bqutil_fns.cw_td_normalize_number(t49.lookup_code) as FLOAT64) <= 24 THEN 'Conservation'
      WHEN upper(t18.lookup_code) LIKE 'C50%'
       AND upper(rtrim(t49.lookup_code)) >= '30'
       AND CAST(bqutil_fns.cw_td_normalize_number(t49.lookup_code) as FLOAT64) <= 80 THEN 'Mastectomy'
      WHEN upper(t18.lookup_code) LIKE 'C50%'
       AND upper(rtrim(t49.lookup_code)) = '90' THEN 'Surgery NOS'
      WHEN upper(t18.lookup_code) LIKE 'C50%'
       AND upper(rtrim(t49.lookup_code)) = '00' THEN 'No Surgery Here'
      ELSE 'Unknown'
    END AS surgical_hospital_type_name,
    coalesce(cptd_cpt_crt_crt.first_surgery_date, cn.surgery_date) AS first_surgery_date,
    cn.general_surgery_type_text,
    t33.lookup_desc AS definitive_palliative_care_summary_desc,
    cn.core_record_type_desc AS recstr_core_record_type_desc,
    cn.recstr_date,
    cn.declined_ind,
    cn.surgical_recstr_physician_name,
    cn.surgery_recstr_side_desc,
    cn.surgery_recstr_type_text,
    t8.lookup_desc AS nodes_examined_desc,
    t9.lookup_desc AS positive_node_desc,
    coalesce(t25.lookup_desc, cn.surgery_side_desc) AS surgical_site_text,
    cptd_cpt_crt_crt.comment_text AS surgical_comment_text,
    cn.core_record_type_desc1 AS surgery_core_record_type_desc,
    coalesce(cptd_cpt_crt_crt.treatment_start_date, cn.surgery_date) AS surgery_date,
    coalesce(rh.hospital_name, cn.surgery_facility_name) AS surgery_facility_name,
    cn.palliative_ind,
    coalesce(concat(cr1.contact_last_name, ',', cr1.contact_first_name), cn.surgical_physician_name) AS surgery_performing_physician_name,
    cn.reconstructive_offered_ind,
    cn.referring_physician_name,
    coalesce(rtt.treatment_type_desc, cn.surgery_type_desc) AS surgery_type_desc,
    coalesce(t24.lookup_desc, cn.margin_result_desc) AS surgical_margin_result_text,
    cn.margin_result_detail_text,
    t50.lookup_desc AS primary_site_surgery_summary_text,
    cn.estrogen_receptor_sw,
    cn.estrogen_receptor_pct_text,
    cn.estrogen_receptor_strength_code,
    cn.oncotype_diagnosis_result_desc,
    cn.oncotype_diagnosis_score_num,
    cn.oncotype_diagnosis_risk_text,
    cn.pathology_tumor_size_available_ind,
    coalesce(cptd_cpt_crt_crt.tumor_size_num_text, cn.tumor_size_num_text) AS tumor_size_num_text,
    cn.nav_result_desc,
    cn.progesterone_receptor_sw,
    cn.progesterone_receptor_strength_code,
    cn.progesterone_receptor_pct_text,
    cptd_cpt_crt_crt.source_system_code,
    cptd_cpt_crt_crt.dw_last_update_date_time
  FROM
    (
      SELECT
          cptd_cpt_crt.cancer_patient_tumor_driver_sk,
          cptd_cpt_crt.cancer_patient_driver_sk,
          cptd_cpt_crt.cancer_tumor_driver_sk,
          cptd_cpt_crt.coid,
          cptd_cpt_crt.company_code,
          cptd_cpt_crt.cr_patient_id,
          cptd_cpt_crt.cn_patient_id,
          cptd_cpt_crt.cp_patient_id,
          cptd_cpt_crt.cr_tumor_primary_site_id,
          cptd_cpt_crt.cn_tumor_type_id,
          cptd_cpt_crt.cp_icd_oncology_code,
          cptd_cpt_crt.source_system_code,
          cptd_cpt_crt.dw_last_update_date_time,
          cptd_cpt_crt.tumor_id,
          cptd_cpt_crt.cr_patient_id_1,
          cptd_cpt_crt.tumor_grade_id,
          cptd_cpt_crt.case_status_id,
          cptd_cpt_crt.tumor_primary_site_id,
          cptd_cpt_crt.definitive_chemo_summary_id,
          cptd_cpt_crt.definitive_diagnostic_stage_summary_id,
          cptd_cpt_crt.definitive_hormone_summary_id,
          cptd_cpt_crt.definitive_immuno_summary_id,
          cptd_cpt_crt.definitive_surgical_margin_summary_id,
          cptd_cpt_crt.definitive_palliative_care_summary_id,
          cptd_cpt_crt.radiation_boost_modality_id,
          cptd_cpt_crt.radiation_hospital_id,
          cptd_cpt_crt.radiation_site_id,
          cptd_cpt_crt.chemo_declined_reason_id,
          cptd_cpt_crt.hormone_declined_reason_id,
          cptd_cpt_crt.immuno_declined_reason_id,
          cptd_cpt_crt.radiation_declined_reason_id,
          cptd_cpt_crt.primary_site_surgery_summary_id,
          cptd_cpt_crt.treatment_therapy_schedule_id,
          cptd_cpt_crt.definitive_radiation_modality_id,
          cptd_cpt_crt.definitive_radiation_volume_summary_id,
          cptd_cpt_crt.regional_lymph_node_summary_id,
          cptd_cpt_crt.surgery_approach_hospital_id,
          cptd_cpt_crt.primary_site_surgery_hospital_id,
          cptd_cpt_crt.definitive_treatment_facility_id,
          cptd_cpt_crt.secondary_treatment_facility_id,
          cptd_cpt_crt.tertiary_treatment_facility_id,
          cptd_cpt_crt.tumor_size_summary_id,
          cptd_cpt_crt.class_case_id,
          cptd_cpt_crt.sequence_primary_id,
          cptd_cpt_crt.best_cs_summary_id,
          cptd_cpt_crt.best_cs_tnm_id,
          cptd_cpt_crt.tumor_size_num_text,
          cptd_cpt_crt.definitive_chemo_date,
          cptd_cpt_crt.definitive_radiation_date,
          cptd_cpt_crt.definitive_immuno_date,
          cptd_cpt_crt.definitive_hormone_date,
          cptd_cpt_crt.definitive_radiation_treatment_end_date,
          cptd_cpt_crt.first_surgery_chemo_elapsed_day_num,
          cptd_cpt_crt.first_surgery_contact_elapsed_day_num,
          cptd_cpt_crt.first_chemo_surgery_elapsed_day_num,
          cptd_cpt_crt.length_to_chemo_day_num,
          cptd_cpt_crt.length_to_hormone_day_num,
          cptd_cpt_crt.length_to_immuno_day_num,
          cptd_cpt_crt.length_to_surgery_day_num,
          cptd_cpt_crt.length_to_radiation_day_num,
          cptd_cpt_crt.length_to_transplant_day_num,
          cptd_cpt_crt.length_to_first_treatment_day_num,
          cptd_cpt_crt.first_surgery_date,
          cptd_cpt_crt.radiation_elapsed_day_num,
          cptd_cpt_crt.definitive_surgery_date,
          cptd_cpt_crt.admission_date,
          cptd_cpt_crt.survival_num,
          cptd_cpt_crt.abstracted_by_text,
          cptd_cpt_crt.managing_physician_code,
          cptd_cpt_crt.medical_oncology_physician_code,
          cptd_cpt_crt.radiation_oncology_physician_code,
          cptd_cpt_crt.primary_surgeon_physician_code,
          cptd_cpt_crt.cs_ss_factor_1_num_code,
          cptd_cpt_crt.cs_ss_factor_2_num_code,
          cptd_cpt_crt.cs_ss_factor_15_num_code,
          cptd_cpt_crt.cs_ss_factor_16_num_code,
          cptd_cpt_crt.cs_ss_factor_22_num_code,
          cptd_cpt_crt.cs_ss_factor_23_num_code,
          cptd_cpt_crt.cs_ss_factor_25_num_code,
          cptd_cpt_crt.mltp_disciplinary_meet_1_present_date,
          cptd_cpt_crt.mltp_disciplinary_meet_1_present_id,
          cptd_cpt_crt.mltp_disciplinary_meet_2_present_date,
          cptd_cpt_crt.mltp_disciplinary_meet_2_present_id,
          cptd_cpt_crt.mltp_disciplinary_meet_3_present_date,
          cptd_cpt_crt.mltp_disciplinary_meet_3_present_id,
          cptd_cpt_crt.source_system_code_1,
          cptd_cpt_crt.dw_last_update_date_time_1,
          cptd_cpt_crt.treatment_id,
          cptd_cpt_crt.tumor_id_1,
          cptd_cpt_crt.treatment_hospital_id,
          cptd_cpt_crt.treatment_type_id,
          cptd_cpt_crt.surgical_site_id,
          cptd_cpt_crt.surgical_margin_result_id,
          cptd_cpt_crt.treatment_type_group_id,
          cptd_cpt_crt.clinical_trial_start_date,
          cptd_cpt_crt.treatment_start_date,
          cptd_cpt_crt.clinical_trial_text,
          cptd_cpt_crt.comment_text,
          cptd_cpt_crt.treatment_performing_physician_code,
          cptd_cpt_crt.source_system_code_1 AS source_system_code_1_0,
          cptd_cpt_crt.dw_last_update_date_time_1 AS dw_last_update_date_time_1_0
        FROM
          cptd_cpt_crt
      UNION ALL
      SELECT
          crt.cancer_patient_tumor_driver_sk,
          crt.cancer_patient_driver_sk,
          crt.cancer_tumor_driver_sk,
          crt.coid,
          crt.company_code,
          crt.cr_patient_id,
          crt.cn_patient_id,
          crt.cp_patient_id,
          crt.cr_tumor_primary_site_id,
          crt.cn_tumor_type_id,
          crt.cp_icd_oncology_code,
          crt.source_system_code,
          crt.dw_last_update_date_time,
          crt.tumor_id,
          crt.cr_patient_id AS cr_patient_id_1,
          crt.tumor_grade_id,
          crt.case_status_id,
          crt.tumor_primary_site_id,
          crt.definitive_chemo_summary_id,
          crt.definitive_diagnostic_stage_summary_id,
          crt.definitive_hormone_summary_id,
          crt.definitive_immuno_summary_id,
          crt.definitive_surgical_margin_summary_id,
          crt.definitive_palliative_care_summary_id,
          crt.radiation_boost_modality_id,
          crt.radiation_hospital_id,
          crt.radiation_site_id,
          crt.chemo_declined_reason_id,
          crt.hormone_declined_reason_id,
          crt.immuno_declined_reason_id,
          crt.radiation_declined_reason_id,
          crt.primary_site_surgery_summary_id,
          crt.treatment_therapy_schedule_id,
          crt.definitive_radiation_modality_id,
          crt.definitive_radiation_volume_summary_id,
          crt.regional_lymph_node_summary_id,
          crt.surgery_approach_hospital_id,
          crt.primary_site_surgery_hospital_id,
          crt.definitive_treatment_facility_id,
          crt.secondary_treatment_facility_id,
          crt.tertiary_treatment_facility_id,
          crt.tumor_size_summary_id,
          crt.class_case_id,
          crt.sequence_primary_id,
          crt.best_cs_summary_id,
          crt.best_cs_tnm_id,
          crt.tumor_size_num_text,
          crt.definitive_chemo_date,
          crt.definitive_radiation_date,
          crt.definitive_immuno_date,
          crt.definitive_hormone_date,
          crt.definitive_radiation_treatment_end_date,
          crt.first_surgery_chemo_elapsed_day_num,
          crt.first_surgery_contact_elapsed_day_num,
          crt.first_chemo_surgery_elapsed_day_num,
          crt.length_to_chemo_day_num,
          crt.length_to_hormone_day_num,
          crt.length_to_immuno_day_num,
          crt.length_to_surgery_day_num,
          crt.length_to_radiation_day_num,
          crt.length_to_transplant_day_num,
          crt.length_to_first_treatment_day_num,
          crt.first_surgery_date,
          crt.radiation_elapsed_day_num,
          crt.definitive_surgery_date,
          crt.admission_date,
          crt.survival_num,
          crt.abstracted_by_text,
          crt.managing_physician_code,
          crt.medical_oncology_physician_code,
          crt.radiation_oncology_physician_code,
          crt.primary_surgeon_physician_code,
          crt.cs_ss_factor_1_num_code,
          crt.cs_ss_factor_2_num_code,
          crt.cs_ss_factor_15_num_code,
          crt.cs_ss_factor_16_num_code,
          crt.cs_ss_factor_22_num_code,
          crt.cs_ss_factor_23_num_code,
          crt.cs_ss_factor_25_num_code,
          crt.mltp_disciplinary_meet_1_present_date,
          crt.mltp_disciplinary_meet_1_present_id,
          crt.mltp_disciplinary_meet_2_present_date,
          crt.mltp_disciplinary_meet_2_present_id,
          crt.mltp_disciplinary_meet_3_present_date,
          crt.mltp_disciplinary_meet_3_present_id,
          crt.source_system_code AS source_system_code_1,
          crt.dw_last_update_date_time AS dw_last_update_date_time_1,
          crt.null_0 AS treatment_id,
          crt.null_1 AS tumor_id_1,
          crt.null_2 AS treatment_hospital_id,
          crt.null_3 AS treatment_type_id,
          crt.null_4 AS surgical_site_id,
          crt.null_5 AS surgical_margin_result_id,
          crt.null_6 AS treatment_type_group_id,
          crt.null_7 AS clinical_trial_start_date,
          crt.null_8 AS treatment_start_date,
          crt.null_9 AS clinical_trial_text,
          crt.null_10 AS comment_text,
          crt.null_11 AS treatment_performing_physician_code,
          crt.null_12 AS source_system_code_1_0,
          crt.null_13 AS dw_last_update_date_time_1_0
        FROM
          crt
    ) AS cptd_cpt_crt_crt
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_hospital AS rh ON rh.hospital_id = cptd_cpt_crt_crt.treatment_hospital_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor_pathology_result AS cptpr ON cptd_cpt_crt_crt.cr_patient_id_1 = cptpr.cr_patient_id
     AND cptd_cpt_crt_crt.tumor_id = cptpr.tumor_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_cr_treatment_type AS rtt ON cptd_cpt_crt_crt.treatment_type_id = rtt.treatment_type_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t18 ON cptd_cpt_crt_crt.tumor_primary_site_id = t18.master_lookup_sid
     AND t18.lookup_id = 18
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t8 ON cptpr.nodes_examined_id = t8.master_lookup_sid
     AND t8.lookup_id = 8
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t9 ON cptpr.positive_node_id = t9.master_lookup_sid
     AND t9.lookup_id = 9
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t33 ON cptd_cpt_crt_crt.definitive_palliative_care_summary_id = t33.master_lookup_sid
     AND t33.lookup_id = 33
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t50 ON cptd_cpt_crt_crt.primary_site_surgery_summary_id = t50.master_lookup_sid
     AND t50.lookup_id = 50
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t49 ON cptd_cpt_crt_crt.primary_site_surgery_hospital_id = t49.master_lookup_sid
     AND t49.lookup_id = 49
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t25 ON cptd_cpt_crt_crt.surgical_site_id = t25.master_lookup_sid
     AND t25.lookup_id = 25
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t24 ON cptd_cpt_crt_crt.surgical_margin_result_id = t24.master_lookup_sid
     AND t24.lookup_id = 24
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t28 ON cptd_cpt_crt_crt.regional_lymph_node_summary_id = t28.master_lookup_sid
     AND t28.lookup_id = 28
    LEFT OUTER JOIN (
      SELECT DISTINCT
          crtt1.treatment_performing_physician_code,
          cpc.contact_first_name,
          cpc.contact_last_name
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crtt1
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact AS cpc ON rtrim(crtt1.treatment_performing_physician_code) = rtrim(cpc.contact_num_code)
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS rlc ON cpc.contact_type_id = rlc.master_lookup_sid
           AND rlc.lookup_id = 10
           AND upper(rtrim(rlc.lookup_code)) = 'D'
    ) AS cr1 ON rtrim(cptd_cpt_crt_crt.treatment_performing_physician_code) = rtrim(cr1.treatment_performing_physician_code)
    LEFT OUTER JOIN (
      SELECT DISTINCT
          cps.nav_patient_id,
          cps.tumor_type_id,
          cps.general_surgery_type_text,
          rcrt.core_record_type_desc,
          cpsr.recstr_date,
          cpsr.declined_ind,
          cpd1.physician_name AS surgical_recstr_physician_name,
          rs.side_desc AS surgery_recstr_side_desc,
          cpsr.surgery_recstr_type_text,
          rs1.side_desc AS surgery_side_desc,
          cps.comment_text,
          rcrt1.core_record_type_desc AS core_record_type_desc1,
          cps.surgery_date,
          rf.facility_name AS surgery_facility_name,
          cps.palliative_ind,
          cpd.physician_name AS surgical_physician_name,
          cps.reconstructive_offered_ind,
          rrp.referring_physician_name,
          cps.surgery_type_id,
          rst.surgery_type_desc,
          cppps.estrogen_receptor_sw,
          cppps.estrogen_receptor_pct_text,
          cppps.estrogen_receptor_strength_code,
          rer.nav_result_desc AS margin_result_desc,
          cppps.margin_result_detail_text,
          cppps.oncotype_diagnosis_score_num,
          cppps.oncotype_diagnosis_risk_text,
          cppps.pathology_tumor_size_available_ind,
          cppps.tumor_size_num_text,
          rer1.nav_result_desc AS nav_result_desc,
          cppps.progesterone_receptor_sw,
          cppps.progesterone_receptor_strength_code,
          cppps.progesterone_receptor_pct_text,
          rer2.nav_result_desc AS oncotype_diagnosis_result_desc
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery AS cps
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppps ON cps.cn_patient_surgery_sid = cppps.cn_patient_procedure_sid
           AND rtrim(cppps.navigation_procedure_type_code) = 'S'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery_reconstruction AS cpsr ON cps.nav_patient_id = cpsr.nav_patient_id
           AND cps.tumor_type_id = cpsr.tumor_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cps.surgery_facility_id = rf.facility_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cps.med_spcl_physician_id = cpd.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd1 ON cpsr.med_spcl_physician_id = cpd1.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpsr.core_record_type_id = rcrt.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt1 ON cps.core_record_type_id = rcrt1.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_surgery_type AS rst ON cps.surgery_type_id = rst.surgery_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs ON cpsr.surgery_recstr_side_id = rs.side_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs1 ON cps.surgery_side_id = rs1.side_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician AS rrp ON cps.referring_physician_id = rrp.referring_physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer ON cppps.margin_result_id = rer.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer1 ON cppps.nav_result_id = rer1.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer2 ON cppps.oncotype_diagnosis_result_id = rer2.nav_result_id
    ) AS cn ON cptd_cpt_crt_crt.cn_patient_id = cn.nav_patient_id
     AND cptd_cpt_crt_crt.cn_tumor_type_id = cn.tumor_type_id
     AND cptd_cpt_crt_crt.treatment_start_date = cn.surgery_date
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.secref_facility AS b ON rtrim(cptd_cpt_crt_crt.company_code) = rtrim(b.company_code)
     AND rtrim(cptd_cpt_crt_crt.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
  WHERE cptd_cpt_crt_crt.tumor_id_1 IS NOT NULL
UNION DISTINCT
SELECT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    CAST(NULL as STRING) AS regional_lymph_node_summary_text,
    substr(NULL, 1, 15) AS surgical_hospital_type_name,
    cn.surgery_date AS first_surgery_date,
    cn.general_surgery_type_text,
    CAST(NULL as STRING) AS definitive_palliative_care_summary_desc,
    cn.core_record_type_desc AS recstr_core_record_type_desc,
    cn.recstr_date,
    cn.declined_ind,
    cn.surgical_recstr_physician_name,
    cn.surgery_recstr_side_desc,
    cn.surgery_recstr_type_text,
    CAST(NULL as STRING) AS nodes_examined_desc,
    CAST(NULL as STRING) AS positive_node_desc,
    substr(cn.surgery_side_desc, 1, 500) AS surgical_site_text,
    substr(NULL, 1, 4000) AS surgical_comment_text,
    cn.core_record_type_desc1 AS surgery_core_record_type_desc,
    cn.surgery_date AS surgery_date,
    cn.surgery_facility_name AS surgery_facility_name,
    cn.palliative_ind,
    cn.surgical_physician_name AS surgery_performing_physician_name,
    cn.reconstructive_offered_ind,
    cn.referring_physician_name,
    substr(cn.surgery_type_desc, 1, 255) AS surgery_type_desc,
    substr(cn.margin_result_desc, 1, 500) AS surgical_margin_result_text,
    cn.margin_result_detail_text,
    CAST(NULL as STRING) AS primary_site_surgery_summary_text,
    cn.estrogen_receptor_sw,
    cn.estrogen_receptor_pct_text,
    cn.estrogen_receptor_strength_code,
    cn.oncotype_diagnosis_result_desc,
    cn.oncotype_diagnosis_score_num,
    cn.oncotype_diagnosis_risk_text,
    cn.pathology_tumor_size_available_ind,
    substr(cn.tumor_size_num_text, 1, 30) AS tumor_size_num_text,
    cn.nav_result_desc,
    cn.progesterone_receptor_sw,
    cn.progesterone_receptor_strength_code,
    cn.progesterone_receptor_pct_text,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN (
      SELECT DISTINCT
          cps.nav_patient_id,
          cps.tumor_type_id,
          cps.general_surgery_type_text,
          rcrt.core_record_type_desc,
          cpsr.recstr_date,
          cpsr.declined_ind,
          cpd1.physician_name AS surgical_recstr_physician_name,
          rs.side_desc AS surgery_recstr_side_desc,
          cpsr.surgery_recstr_type_text,
          rs1.side_desc AS surgery_side_desc,
          cps.comment_text,
          rcrt1.core_record_type_desc AS core_record_type_desc1,
          cps.surgery_date,
          rf.facility_name AS surgery_facility_name,
          cps.palliative_ind,
          cpd.physician_name AS surgical_physician_name,
          cps.reconstructive_offered_ind,
          rrp.referring_physician_name,
          cps.surgery_type_id,
          rst.surgery_type_desc,
          cppps.estrogen_receptor_sw,
          cppps.estrogen_receptor_pct_text,
          cppps.estrogen_receptor_strength_code,
          rer.nav_result_desc AS margin_result_desc,
          cppps.margin_result_detail_text,
          cppps.oncotype_diagnosis_score_num,
          cppps.oncotype_diagnosis_risk_text,
          cppps.pathology_tumor_size_available_ind,
          cppps.tumor_size_num_text,
          rer1.nav_result_desc AS nav_result_desc,
          cppps.progesterone_receptor_sw,
          cppps.progesterone_receptor_strength_code,
          cppps.progesterone_receptor_pct_text,
          rer2.nav_result_desc AS oncotype_diagnosis_result_desc
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery AS cps
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppps ON cps.cn_patient_surgery_sid = cppps.cn_patient_procedure_sid
           AND rtrim(cppps.navigation_procedure_type_code) = 'S'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_surgery_reconstruction AS cpsr ON cps.nav_patient_id = cpsr.nav_patient_id
           AND cps.tumor_type_id = cpsr.tumor_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cps.surgery_facility_id = rf.facility_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cps.med_spcl_physician_id = cpd.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd1 ON cpsr.med_spcl_physician_id = cpd1.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpsr.core_record_type_id = rcrt.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt1 ON cps.core_record_type_id = rcrt1.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_surgery_type AS rst ON cps.surgery_type_id = rst.surgery_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs ON cpsr.surgery_recstr_side_id = rs.side_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_side AS rs1 ON cps.surgery_side_id = rs1.side_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician AS rrp ON cps.referring_physician_id = rrp.referring_physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer ON cppps.margin_result_id = rer.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer1 ON cppps.nav_result_id = rer1.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer2 ON cppps.oncotype_diagnosis_result_id = rer2.nav_result_id
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
    INNER JOIN {{ params.param_cr_base_views_dataset_name }}.secref_facility AS b ON rtrim(cptd.company_code) = rtrim(b.company_code)
     AND rtrim(cptd.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
  WHERE (cptd.cn_patient_id, cptd.cn_tumor_type_id, cn.surgery_date) NOT IN(
    SELECT AS STRUCT
        coalesce(cptd_0.cn_patient_id, CAST(-99 as NUMERIC)),
        coalesce(cptd_0.cn_tumor_type_id, -99),
        coalesce(crt.treatment_start_date, DATE '9999-12-31')
      FROM
        {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd_0
        LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd_0.cr_patient_id = cpt.cr_patient_id
         AND cptd_0.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
        LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crt ON cpt.tumor_id = crt.tumor_id
  )