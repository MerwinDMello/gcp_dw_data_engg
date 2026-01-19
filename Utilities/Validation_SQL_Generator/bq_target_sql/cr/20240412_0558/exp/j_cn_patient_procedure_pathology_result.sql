-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_procedure_pathology_result.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    (
      SELECT
          row_number() OVER (ORDER BY stg.cn_patient_procedure_sid, rre.nav_result_id, upper(stg.navigation_procedure_type_code), stg.pathology_result_date, upper(stg.pathology_result_name), upper(stg.pathology_grade_available_ind), stg.pathology_grade_num, upper(stg.pathology_tumor_size_av_ind), upper(stg.tumor_size_num_text), upper(stg.margin_result_detail_text), upper(stg.sentinel_node_result_code), stg.estrogen_receptor_sw, upper(stg.estrogen_receptor_st_cd), upper(stg.estrogen_receptor_pct_text), stg.progesterone_receptor_sw, upper(stg.progesterone_receptor_st_cd), upper(stg.progesterone_receptor_pct_text), upper(stg.oncotype_diagnosis_score_num), upper(stg.oncotype_diagnosis_risk_text), upper(stg.comment_text), upper(stg.hashbite_ssk)) + (
            SELECT
                coalesce(max(cn_patient_procedure_pathology_result.cn_patient_proc_pathology_result_sid), 0) AS id1
              FROM
                `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result
          ) AS cn_patient_proc_pathology_result_sid,
          stg.cn_patient_procedure_sid AS cn_patient_procedure_sid,
          rre.nav_result_id AS margin_result_id,
          rre.nav_result_id AS nav_result_id,
          rre.nav_result_id AS oncotype_diagnosis_result_id,
          stg.navigation_procedure_type_code,
          stg.pathology_result_date,
          stg.pathology_result_name,
          stg.pathology_grade_available_ind,
          stg.pathology_grade_num,
          stg.pathology_tumor_size_av_ind,
          stg.tumor_size_num_text,
          stg.margin_result_detail_text,
          stg.sentinel_node_result_code,
          stg.estrogen_receptor_sw,
          stg.estrogen_receptor_st_cd,
          stg.estrogen_receptor_pct_text,
          stg.progesterone_receptor_sw,
          stg.progesterone_receptor_st_cd,
          stg.progesterone_receptor_pct_text,
          stg.oncotype_diagnosis_score_num,
          stg.oncotype_diagnosis_risk_text,
          stg.comment_text,
          stg.hashbite_ssk,
          stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_procedure_pathology_result_stg AS stg
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_result AS rre ON upper(rtrim(stg.nav_result_id)) = upper(rtrim(rre.nav_result_desc))
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_result AS rre1 ON upper(rtrim(stg.margin_result_id)) = upper(rtrim(rre1.nav_result_desc))
          LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_result AS rre2 ON upper(rtrim(stg.oncotype_diagnosis_result_id)) = upper(rtrim(rre2.nav_result_desc))
        WHERE (upper(stg.hashbite_ssk), upper(stg.navigation_procedure_type_code)) NOT IN(
          SELECT AS STRUCT
              upper(cn_patient_procedure_pathology_result.hashbite_ssk) AS hashbite_ssk,
              upper(cn_patient_procedure_pathology_result.navigation_procedure_type_code) AS navigation_procedure_type_code
            FROM
              `hca-hin-dev-cur-ops`.edwcr.cn_patient_procedure_pathology_result
        )
         AND stg.dw_last_update_date_time < (
          SELECT
              max(etl_job_run.job_start_date_time) AS job_start_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_dmx_ac_base_views.etl_job_run
            WHERE upper(rtrim(etl_job_run.job_name)) = 'J_CN_PATIENT_PROCEDURE_PATHOLOGY_RESULT'
        )
    ) AS src
;
