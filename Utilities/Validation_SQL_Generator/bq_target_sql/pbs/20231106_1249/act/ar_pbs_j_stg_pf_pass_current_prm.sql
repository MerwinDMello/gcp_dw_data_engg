-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ar_pbs_j_stg_pf_pass_current_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', coalesce(count(*), 0)) AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_staging.pass_current_pf AS pc
  WHERE upper(pc.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
   AND pc.dw_last_update_date_time IN(
    SELECT
        max(pass_current_pf.dw_last_update_date_time)
      FROM
        `hca-hin-dev-cur-parallon`.edwpbs_staging.pass_current_pf
  )
;
