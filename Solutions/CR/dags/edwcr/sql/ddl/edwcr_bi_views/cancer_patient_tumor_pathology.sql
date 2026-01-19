-- Translation time: 2023-05-10T05:47:50.588331Z
-- Translation job ID: 92e92d81-1143-4781-91d8-614cfb5dfe89
-- Source: eim-ops-cs-datamig-dev-0002/sql_conversion/edwcr_migration_source/edwcr_bi_views/cancer_patient_tumor_pathology.sql
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_cr_bi_views_dataset_name }}.cancer_patient_tumor_pathology AS SELECT DISTINCT
    cptd.cancer_patient_tumor_driver_sk,
    cptd.cancer_patient_driver_sk,
    cptd.cancer_tumor_driver_sk,
    cptd.coid,
    cptd.company_code,
    cnprs.kras_text,
    cnprs.braf_text,
    cnprs.msi_text,
    cnprs.ki67results_text,
    cnprs.ki67_text,
    cnprs.cmet_text,
    cnprs.signet_ring_text,
    cnprs.linitis_plastica_text,
    cnprs.non_small_cell_name,
    cnprs.pancoast_text,
    cnprs.alk_text,
    cnprs.egfr_text,
    cnprs.ros_text,
    cnprs.pik3ca_text,
    cnprs.met_text,
    cnprs.her2neuihc_text,
    cnprs.fish_text,
    cnprs.axillary_nodes_tested_text,
    cnprs.mucinous_text,
    cnprs.comedonecrosis_text,
    cnprs.fish_equivocal_text,
    cnprs.any_local_event_dcis_inv_pct_text,
    cnprs.ten_year_recurrence_rate_text,
    cnprs.invasive_local_event_pct_text,
    cnprs.erpr_text,
    rcrt.core_record_type_desc,
    cptd.source_system_code,
    cptd.dw_last_update_date_time
  FROM
    {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd
    INNER JOIN (
      SELECT
          cn_patient_pathology_result.cn_patient_pathology_result_sid,
          cn_patient_pathology_result.nav_patient_id,
          cn_patient_pathology_result.tumor_type_id,
          cn_patient_pathology_result.core_record_type_id,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 1 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS kras_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 2 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS braf_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 3 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS msi_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 4 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS ki67results_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 5 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS ki67_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 6 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS cmet_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 7 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS signet_ring_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 8 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS linitis_plastica_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 9 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS non_small_cell_name,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 10 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS pancoast_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 11 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS alk_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 12 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS egfr_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 13 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS ros_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 14 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS pik3ca_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 15 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS met_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 16 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS her2neuihc_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 17 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS fish_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 18 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS axillary_nodes_tested_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 19 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS mucinous_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 20 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS comedonecrosis_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 21 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS fish_equivocal_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 22 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS any_local_event_dcis_inv_pct_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 23 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS ten_year_recurrence_rate_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 24 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS invasive_local_event_pct_text,
          max(CAST(CASE
            WHEN cn_patient_pathology_result.pathology_result_type_id = 25 THEN cn_patient_pathology_result.result_value_text
            ELSE NULL
          END as INT64)) AS erpr_text
        FROM
          {{ params.param_cr_base_views_dataset_name }}.cn_patient_pathology_result
        GROUP BY 1, 2, 3, 4
    ) AS cnprs ON cptd.cn_patient_id = cnprs.nav_patient_id
     AND cptd.cn_tumor_type_id = cnprs.tumor_type_id
    LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_core_record_type AS rcrt ON cnprs.core_record_type_id = rcrt.core_record_type_id
    INNER JOIN {{ params.param_auth_base_views_dataset_name }}.cr_secref_facility AS b ON cptd.company_code = b.company_code
     AND cptd.coid = b.co_id
     AND b.user_id = session_user()
;
