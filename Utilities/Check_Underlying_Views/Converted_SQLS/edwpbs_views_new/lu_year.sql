-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
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
