-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_armap_company_goal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_company_goal AS SELECT
    lu_armap_company_goal.year_id,
    lu_armap_company_goal.corporate_code,
    lu_armap_company_goal.coid,
    lu_armap_company_goal.metric_code,
    lu_armap_company_goal.metric_value,
    lu_armap_company_goal.dw_last_update_date_time,
    lu_armap_company_goal.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_armap_company_goal
;
