-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
