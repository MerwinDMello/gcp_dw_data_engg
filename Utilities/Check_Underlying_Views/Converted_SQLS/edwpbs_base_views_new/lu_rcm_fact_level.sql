-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_rcm_fact_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_fact_level AS SELECT
    lu_rcm_fact_level.service_type_name,
    lu_rcm_fact_level.fact_lvl_code,
    lu_rcm_fact_level.parent_name,
    lu_rcm_fact_level.parent_short_name,
    lu_rcm_fact_level.child_name,
    lu_rcm_fact_level.child_short_name,
    lu_rcm_fact_level.dw_last_update_date_time,
    lu_rcm_fact_level.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_rcm_fact_level
;
