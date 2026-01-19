-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_rcm_group.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_group AS SELECT
    lu_rcm_group.group_code,
    lu_rcm_group.corporate_code,
    lu_rcm_group.group_name,
    lu_rcm_group.b_level,
    lu_rcm_group.dw_last_update_date_time,
    lu_rcm_group.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_group
;
