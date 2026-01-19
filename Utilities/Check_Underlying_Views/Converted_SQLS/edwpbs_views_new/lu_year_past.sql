-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_year_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_year_past AS SELECT DISTINCT
    substr(substr(CAST(lu_month.month_id as STRING), 1, 6), 1, 4) AS current_year,
    substr(substr(CAST(lu_month.month_id - 100 as STRING), 1, 6), 1, 4) AS prior_year,
    substr(substr(CAST(lu_month.month_id - 200 as STRING), 1, 6), 1, 4) AS prior_2_year,
    substr(substr(CAST(lu_month.month_id - 300 as STRING), 1, 6), 1, 4) AS prior_3_year
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month
;
