-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_nbt_provider_spcl_cat.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_nbt_provider_spcl_cat AS SELECT
    ref_nbt_provider_spcl_cat.nbt_spcl_cat_id,
    ref_nbt_provider_spcl_cat.nbt_spcl_cat_desc,
    ref_nbt_provider_spcl_cat.source_system_code,
    ref_nbt_provider_spcl_cat.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.ref_nbt_provider_spcl_cat
;
