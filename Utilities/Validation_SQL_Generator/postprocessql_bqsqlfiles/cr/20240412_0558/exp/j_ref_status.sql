-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_ref_status.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT trim(ref_status_stg.status_desc) AS status_desc,
          trim(ref_status_stg.status_type_desc) AS status_type_desc,
          ref_status_stg.source_system_code AS source_system_code
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.ref_status_stg
   WHERE (upper(trim(ref_status_stg.status_desc)),
          upper(trim(ref_status_stg.status_type_desc))) NOT IN
       (SELECT AS STRUCT upper(trim(ref_status.status_desc)),
                         upper(trim(ref_status.status_type_desc))
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.ref_status)
     AND ref_status_stg.dw_last_update_date_time <
       (SELECT max(etl_job_run.job_start_date_time) AS job_start_date_time
        FROM `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
        WHERE upper(rtrim(etl_job_run.job_name)) = 'J_REF_STATUS' ) ) AS a