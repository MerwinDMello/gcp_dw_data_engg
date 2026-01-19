-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_rcm_org_level.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_org_level AS SELECT
    dim_rcm_org_level.service_type_name,
    dim_rcm_org_level.fact_lvl_code,
    dim_rcm_org_level.child_fact_lvl_code,
    dim_rcm_org_level.parent_code,
    dim_rcm_org_level.child_code,
    dim_rcm_org_level.parent_desc,
    dim_rcm_org_level.child_desc,
    dim_rcm_org_level.dw_last_update_date_time,
    dim_rcm_org_level.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_org_level
;
