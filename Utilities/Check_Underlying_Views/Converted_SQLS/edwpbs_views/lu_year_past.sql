-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_year_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_year_past AS SELECT DISTINCT
    substr(CAST(lu_month.month_id as STRING), 1, 4) AS current_year,
    substr(CAST(lu_month.month_id - 100 as STRING), 1, 4) AS prior_year,
    substr(CAST(lu_month.month_id - 200 as STRING), 1, 4) AS prior_2_year,
    substr(CAST(lu_month.month_id - 300 as STRING), 1, 4) AS prior_3_year
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month
;
