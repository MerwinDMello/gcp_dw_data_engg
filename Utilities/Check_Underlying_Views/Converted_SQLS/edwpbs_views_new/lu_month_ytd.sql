-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_month_ytd.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_month_ytd AS SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE upper(trim(substr(CAST(lu_month_rolling.child_month_id as STRING), 1, 4))) = upper(trim(substr(CAST(lu_month_rolling.parent_month_id as STRING), 1, 4)))
;
