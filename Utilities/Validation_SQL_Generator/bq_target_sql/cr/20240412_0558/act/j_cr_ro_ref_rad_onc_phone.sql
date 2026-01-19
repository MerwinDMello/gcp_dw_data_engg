-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/act/j_cr_ro_ref_rad_onc_phone.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    `hca-hin-dev-cur-ops`.edwcr.rad_onc_phone
  WHERE rad_onc_phone.dw_last_update_date_time >= (
    SELECT
        max(etl_job_run.job_start_date_time) AS job_start_date_time
      FROM
        `hca-hin-dev-cur-ops`.edwcr_dmx_ac.etl_job_run
      WHERE upper(rtrim(etl_job_run.job_name)) = 'J_CR_RO_REF_RAD_ONC_PHONE'
  )
;
