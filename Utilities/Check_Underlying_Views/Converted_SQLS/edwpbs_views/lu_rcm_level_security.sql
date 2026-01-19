-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_rcm_level_security.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_level_security AS SELECT
    lu_rcm_level_security.service_type_name,
    lu_rcm_level_security.fact_lvl_code,
    lu_rcm_level_security.parent_code,
    lu_rcm_level_security.coid,
    lu_rcm_level_security.parent_desc,
    lu_rcm_level_security.dw_last_update_date_time,
    lu_rcm_level_security.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_level_security
;
