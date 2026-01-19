-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
  WHERE upper(trim(CAST(lu_month_rolling.child_month_id as STRING))) = upper(trim(CAST(lu_month_rolling.parent_month_id as STRING)))
;
