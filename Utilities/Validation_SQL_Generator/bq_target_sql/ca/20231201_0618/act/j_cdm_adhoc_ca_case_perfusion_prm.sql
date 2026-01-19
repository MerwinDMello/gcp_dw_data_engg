-- Translation time: 2023-12-01T12:21:28.647193Z
-- Translation job ID: 4144eabf-8284-495a-81e3-de22d755910b
-- Source: eim-clin-pdoc-ccda-dev-0001/ca_bulk_conversion_validation/20231201_0618/input/act/j_cdm_adhoc_ca_case_perfusion_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    coalesce(count(*), 0)
  FROM
    `hca-hin-dev-cur-clinical`.edwcdm.ca_case_perfusion
  WHERE ca_case_perfusion.dw_last_update_date_time >= (
    SELECT
        max(etl_job_run.job_start_date_time) AS job_start_date_time
      FROM
        `hca-hin-dev-cur-clinical`.edwcdm_ac.etl_job_run
      WHERE upper(etl_job_run.job_name) = 'J_CDM_ADHOC_CA_CASE_PERFUSION'
  )
;
