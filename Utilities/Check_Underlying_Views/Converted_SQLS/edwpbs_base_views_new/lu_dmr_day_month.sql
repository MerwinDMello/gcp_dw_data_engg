-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_dmr_day_month.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_dmr_day_month AS SELECT
    lu_dmr_day_month.dmr_day_month_ind,
    lu_dmr_day_month.dmr_day_month_desc,
    lu_dmr_day_month.dw_last_update_date_time,
    lu_dmr_day_month.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_dmr_day_month
;
