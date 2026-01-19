-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_date_rolling.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_date_rolling AS SELECT
    lu_date_rolling.date_id,
    lu_date_rolling.child_date_id,
    lu_date_rolling.day_of_week_desc,
    lu_date_rolling.dw_last_update_date_time,
    lu_date_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_date_rolling
;
