-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_year_month_past.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_year_month_past AS SELECT
    lu_year_month_past.month_id,
    lu_year_month_past.month_desc_s,
    lu_year_month_past.prior_month_id,
    lu_year_month_past.prior_2_month_id,
    lu_year_month_past.prior_3_month_id,
    lu_year_month_past.prior_year_month_id,
    lu_year_month_past.prior_2_year_month_id,
    lu_year_month_past.prior_3_year_month_id,
    lu_year_month_past.current_month_flag,
    lu_year_month_past.curent_year,
    lu_year_month_past.prior_year,
    lu_year_month_past.prior_2_year,
    lu_year_month_past.prior_3_year,
    lu_year_month_past.current_year_flag,
    lu_year_month_past.prev_month_days,
    lu_year_month_past.prev_2_month_days,
    lu_year_month_past.prev_3_month_days,
    lu_year_month_past.month_36_ind,
    lu_year_month_past.month_13_ind,
    lu_year_month_past.dw_last_update_date_time,
    lu_year_month_past.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_year_month_past
;
