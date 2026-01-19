-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
