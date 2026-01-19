-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_year.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_year AS SELECT
    lu_year.year_id
  FROM
    `hca-hin-dev-cur-parallon`.edw_pub_views.lu_year
  WHERE lu_year.year_id <= extract(YEAR from current_date('US/Central'))
   OR lu_year.year_id = 9999
;
