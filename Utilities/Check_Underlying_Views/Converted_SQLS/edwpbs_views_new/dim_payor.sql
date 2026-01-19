-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_payor.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_payor AS SELECT
    dim_payor.payor_sid,
    dim_payor.payor_id,
    coalesce(dim_payor.payor_name, 'No_Payor') AS payor_name,
    coalesce(dim_payor.payor_short_name, 'No_Payor') AS payor_short_name,
    dim_payor.payor_type,
    dim_payor.dw_last_update_date_time,
    dim_payor.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_payor
;
