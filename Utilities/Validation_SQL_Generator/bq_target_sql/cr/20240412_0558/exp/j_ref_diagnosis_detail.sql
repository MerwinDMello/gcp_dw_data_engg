-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_diagnosis_detail.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          trim(ref_diagnosis_detail_stg.diagnosis_detail_desc) AS diagnosis_detail_desc,
          trim(ref_diagnosis_detail_stg.diagnosis_indicator_text) AS diagnosis_indicator_text,
          ref_diagnosis_detail_stg.source_system_code AS source_system_code
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.ref_diagnosis_detail_stg
        WHERE (upper(trim(ref_diagnosis_detail_stg.diagnosis_detail_desc)), upper(trim(ref_diagnosis_detail_stg.diagnosis_indicator_text))) NOT IN(
          SELECT AS STRUCT
              upper(trim(ref_diagnosis_detail.diagnosis_detail_desc)),
              upper(trim(ref_diagnosis_detail.diagnosis_indicator_text))
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_diagnosis_detail
        )
         AND ref_diagnosis_detail_stg.dw_last_update_date_time < (
          SELECT
              max(etl_job_run.job_start_date_time) AS job_start_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
            WHERE upper(rtrim(etl_job_run.job_name)) = 'J_REF_DIAGNOSIS_DETAIL'
        )
    ) AS a
;
