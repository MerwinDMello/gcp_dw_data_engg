-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_payor AS SELECT
    dim_payor.payor_sid,
    dim_payor.payor_id,
    dim_payor.payor_name,
    dim_payor.payor_short_name,
    dim_payor.payor_type,
    dim_payor.dw_last_update_date_time,
    dim_payor.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_payor
;
