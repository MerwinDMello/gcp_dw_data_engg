-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cr_ro_rad_onc_actvt_priority.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT DISTINCT CASE
                       WHEN upper(trim(dhd.activitypriority)) = '' THEN CAST(NULL AS STRING)
                       ELSE trim(dhd.activitypriority)
                   END AS activity_priority_desc,
                   'R' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimactivitytransaction AS dhd) AS src
WHERE src.activity_priority_desc IS NOT NULL