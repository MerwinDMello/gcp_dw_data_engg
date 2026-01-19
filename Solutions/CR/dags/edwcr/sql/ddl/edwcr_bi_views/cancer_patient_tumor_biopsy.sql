-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_biopsy.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_biopsy AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cn.biopsy_clip_sw,
    cn.comment_text AS biopsy_comment_text,
    cn.core_record_type_desc AS biopsy_core_record_desc,
    coalesce(cr.treatment_start_date, cn.biopsy_date) AS biopsy_date,
    coalesce(cr.hospital_name, cn.facility_name) AS biopsy_facility_name,
    cn.biopsy_needle_sw,
    cn.physician_name AS biopsy_performing_physician_name,
    cn.physician_specialty_desc AS biopsy_physician_specialty_desc,
    cn.referring_physician_name AS biopsy_referring_physician_name,
    cn.biopsy_result_desc,
    cn.biopsy_site_location_name,
    cn.biopsy_type_desc,
    cn.general_biopsy_type_text,
    cn.estrogen_receptor_sw,
    cn.estrogen_receptor_pct_text,
    cn.estrogen_receptor_strength_code,
    coalesce(t24.lookup_desc, cn.margin_result_desc) AS margin_result_text,
    cn.margin_result_detail_text,
    t8.lookup_desc AS nodes_examined_desc,
    t9.lookup_desc AS positive_node_desc,
    cn.oncotype_diagnosis_result_desc,
    cn.oncotype_diagnosis_score_num,
    cn.oncotype_diagnosis_risk_text,
    cn.pathology_tumor_size_available_ind,
    coalesce(cr.tumor_size_num_text, cn.tumor_size_num_text) AS tumor_size_num_text,
    cn.progesterone_receptor_sw,
    cn.progesterone_receptor_strength_code,
    cn.progesterone_receptor_pct_text,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    LEFT OUTER JOIN (
      SELECT
          rh.hospital_name,
          crt.treatment_start_date,
          cpt.tumor_primary_site_id,
          cpt.cr_patient_id,
          crt.surgical_margin_result_id,
          cptpr.nodes_examined_id,
          cptpr.positive_node_id,
          cpt.tumor_size_num_text
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crt ON cpt.tumor_id = crt.tumor_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_hospital AS rh ON rh.hospital_id = crt.treatment_hospital_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor_pathology_result AS cptpr ON cpt.cr_patient_id = cptpr.cr_patient_id
           AND cpt.tumor_id = cptpr.tumor_id
        WHERE treatment_type_group_id IN(
          SELECT
              treatment_type_group_id
            FROM
              {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group
            WHERE upper(treatment_type_group_code) = 'D'
        )
    ) AS cr ON cptd.cr_patient_id = cr.cr_patient_id
     AND cptd.cr_tumor_primary_site_id = cr.tumor_primary_site_id
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t24 ON cr.surgical_margin_result_id = t24.master_lookup_sid
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t8 ON cr.nodes_examined_id = t8.master_lookup_sid
     AND t8.lookup_id = 8
    LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_lookup_code AS t9 ON cr.positive_node_id = t9.master_lookup_sid
     AND t9.lookup_id = 9
    LEFT OUTER JOIN (
      SELECT
          cpb.biopsy_clip_sw,
          cpb.comment_text,
          rcrt.core_record_type_desc,
          cpb.biopsy_date,
          cpb.biopsy_facility_id,
          cpb.biopsy_needle_sw,
          cpd.physician_name,
          rps.physician_specialty_desc,
          rrp.referring_physician_name,
          rbr.biopsy_result_desc,
          rsl.site_location_desc AS biopsy_site_location_name,
          rbt.biopsy_type_desc,
          cpb.general_biopsy_type_text,
          rf.facility_name,
          cpb.nav_patient_id,
          cpb.tumor_type_id,
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
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_biopsy AS cpb
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppps ON cpb.cn_patient_biopsy_sid = cppps.cn_patient_procedure_sid
           AND upper(navigation_procedure_type_code) = 'B'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cpb.biopsy_facility_id = rf.facility_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_physician_specialty AS rps ON cpb.biopsy_physician_specialty_id = rps.physician_specialty_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_site_location AS rsl ON cpb.biopsy_site_location_id = rsl.site_location_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_result AS rbr ON cpb.biopsy_result_id = rbr.biopsy_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cpb.med_spcl_physician_id = cpd.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_role AS cpr ON cpd.physician_id = cpr.physician_id
           AND upper(physician_role_code) = 'BP'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpb.core_record_type_id = rcrt.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_type AS rbt ON cpb.biopsy_type_id = rbt.biopsy_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician AS rrp ON cpb.referring_physician_id = rrp.referring_physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer ON cppps.margin_result_id = rer.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer1 ON cppps.nav_result_id = rer1.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer2 ON cppps.oncotype_diagnosis_result_id = rer2.nav_result_id
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
     AND cr.treatment_start_date = cn.biopsy_date
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE cr.cr_patient_id IS NOT NULL
UNION ALL
SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cn.biopsy_clip_sw,
    cn.comment_text AS biopsy_comment_text,
    cn.core_record_type_desc AS biopsy_core_record_desc,
    cn.biopsy_date AS biopsy_date,
    cn.facility_name AS biopsy_facility_name,
    cn.biopsy_needle_sw,
    cn.physician_name AS biopsy_performing_physician_name,
    cn.physician_specialty_desc AS biopsy_physician_specialty_desc,
    cn.referring_physician_name AS biopsy_referring_physician_name,
    cn.biopsy_result_desc,
    cn.biopsy_site_location_name,
    cn.biopsy_type_desc,
    cn.general_biopsy_type_text,
    cn.estrogen_receptor_sw,
    cn.estrogen_receptor_pct_text,
    cn.estrogen_receptor_strength_code,
    cn.margin_result_desc AS margin_result_text,
    cn.margin_result_detail_text,
    CAST(NULL as STRING) AS nodes_examined_desc,
    CAST(NULL as STRING) AS positive_node_desc,
    cn.oncotype_diagnosis_result_desc,
    cn.oncotype_diagnosis_score_num,
    cn.oncotype_diagnosis_risk_text,
    cn.pathology_tumor_size_available_ind,
    cn.tumor_size_num_text,
    cn.progesterone_receptor_sw,
    cn.progesterone_receptor_strength_code,
    cn.progesterone_receptor_pct_text,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN (
      SELECT
          cpb.biopsy_clip_sw,
          cpb.comment_text,
          rcrt.core_record_type_desc,
          cpb.biopsy_date,
          cpb.biopsy_facility_id,
          cpb.biopsy_needle_sw,
          cpd.physician_name,
          rps.physician_specialty_desc,
          rrp.referring_physician_name,
          rbr.biopsy_result_desc,
          rsl.site_location_desc AS biopsy_site_location_name,
          rbt.biopsy_type_desc,
          cpb.general_biopsy_type_text,
          rf.facility_name,
          cpb.nav_patient_id,
          cpb.tumor_type_id,
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
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_biopsy AS cpb
          INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_patient_procedure_pathology_result AS cppps ON cpb.cn_patient_biopsy_sid = cppps.cn_patient_procedure_sid
           AND upper(navigation_procedure_type_code) = 'B'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_facility AS rf ON cpb.biopsy_facility_id = rf.facility_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_physician_specialty AS rps ON cpb.biopsy_physician_specialty_id = rps.physician_specialty_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_site_location AS rsl ON cpb.biopsy_site_location_id = rsl.site_location_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_result AS rbr ON cpb.biopsy_result_id = rbr.biopsy_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_detail AS cpd ON cpb.med_spcl_physician_id = cpd.physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cn_physician_role AS cpr ON cpd.physician_id = cpr.physician_id
           AND upper(physician_role_code) = 'BP'
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_core_record_type AS rcrt ON cpb.core_record_type_id = rcrt.core_record_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_biopsy_type AS rbt ON cpb.biopsy_type_id = rbt.biopsy_type_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_referring_physician AS rrp ON cpb.referring_physician_id = rrp.referring_physician_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer ON cppps.margin_result_id = rer.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer1 ON cppps.nav_result_id = rer1.nav_result_id
          LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_result AS rer2 ON cppps.oncotype_diagnosis_result_id = rer2.nav_result_id
    ) AS cn ON cptd.cn_patient_id = cn.nav_patient_id
     AND cptd.cn_tumor_type_id = cn.tumor_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
  WHERE (cptd.cn_patient_id, cptd.cn_tumor_type_id, cn.biopsy_date) NOT IN(
    SELECT AS STRUCT
        coalesce(cptd_0.cn_patient_id, -99),
        coalesce(cptd_0.cn_tumor_type_id, -99),
        coalesce(crt.treatment_start_date, DATE '9999-12-31')
      FROM
        {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd_0
        LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt ON cptd_0.cr_patient_id = cpt.cr_patient_id
         AND cptd_0.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
        LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_treatment AS crt ON cpt.tumor_id = crt.tumor_id
      WHERE treatment_type_group_id IN(
        SELECT
            treatment_type_group_id
          FROM
            {{ params.param_cr_base_views_dataset_name }}.ref_treatment_type_group
          WHERE upper(treatment_type_group_code) = 'D'
      )
  )
;
