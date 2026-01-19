-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/ep_j_ep_remittance_service_rarc_ac.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    format('%20d', count(*)) AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_service_rarc
  WHERE remittance_service_rarc.audit_date = (
    SELECT
        max(remittance_service_rarc_0.audit_date)
      FROM
        `hca-hin-dev-cur-parallon`.edwpbs_staging.remittance_service_rarc AS remittance_service_rarc_0
  )
;
