-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_therapy_type.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', count(*)), ',\t') AS source_string
  FROM
    (
      SELECT
          therapy_type_stg.therapy_type_desc,
          therapy_type_stg.source_system_code
        FROM
          `hca-hin-dev-cur-ops`.edwcr_staging.therapy_type_stg
        WHERE upper(trim(therapy_type_stg.therapy_type_desc)) NOT IN(
          SELECT
              upper(trim(ref_therapy_type.therapy_type_desc))
            FROM
              `hca-hin-dev-cur-ops`.edwcr.ref_therapy_type
        )
         AND therapy_type_stg.dw_last_update_date_time < (
          SELECT
              max(etl_job_run.job_start_date_time) AS job_start_date_time
            FROM
              `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
            WHERE upper(rtrim(etl_job_run.job_name)) = 'J_REF_THERAPY_TYPE'
        )
    ) AS a
;
