-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_armap_company_goal.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_armap_company_goal AS SELECT
    lu_armap_company_goal.year_id,
    lu_armap_company_goal.corporate_code,
    lu_armap_company_goal.coid,
    lu_armap_company_goal.metric_code,
    ROUND(lu_armap_company_goal.metric_value, 5, 'ROUND_HALF_EVEN') AS metric_value,
    lu_armap_company_goal.dw_last_update_date_time,
    lu_armap_company_goal.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_armap_company_goal
;
