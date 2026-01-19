-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_month_rolling.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_month_rolling AS SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) = 1
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) IN(
    CAST(8 as FLOAT64), CAST(9 as FLOAT64), CAST(10 as FLOAT64), CAST(11 as FLOAT64), CAST(12 as FLOAT64), CAST(1 as FLOAT64)
  )
UNION DISTINCT
SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) = 2
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) IN(
    CAST(9 as FLOAT64), CAST(10 as FLOAT64), CAST(11 as FLOAT64), CAST(12 as FLOAT64), CAST(1 as FLOAT64), CAST(2 as FLOAT64)
  )
UNION DISTINCT
SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) = 3
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) IN(
    CAST(10 as FLOAT64), CAST(11 as FLOAT64), CAST(12 as FLOAT64), CAST(1 as FLOAT64), CAST(2 as FLOAT64), CAST(3 as FLOAT64)
  )
UNION DISTINCT
SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) = 4
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) IN(
    CAST(11 as FLOAT64), CAST(12 as FLOAT64), CAST(1 as FLOAT64), CAST(2 as FLOAT64), CAST(3 as FLOAT64), CAST(4 as FLOAT64)
  )
UNION DISTINCT
SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) = 5
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) IN(
    CAST(12 as FLOAT64), CAST(1 as FLOAT64), CAST(2 as FLOAT64), CAST(3 as FLOAT64), CAST(4 as FLOAT64), CAST(5 as FLOAT64)
  )
UNION DISTINCT
SELECT
    lu_month_rolling.child_month_id,
    lu_month_rolling.parent_month_id,
    lu_month_rolling.current_roll_month_flag,
    lu_month_rolling.parent_month_desc,
    lu_month_rolling.dw_last_update_date_time,
    lu_month_rolling.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_month_rolling
  WHERE CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) >= 6
   AND CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.child_month_id), 5, 2)) as FLOAT64) >= CAST(bqutil.fn.cw_td_normalize_number(substr(trim(lu_month_rolling.parent_month_id), 5, 2)) as FLOAT64) - 5
   AND upper(trim(CAST(/* expression of unknown or erroneous type */ lu_month_rolling.child_month_id as STRING))) = upper(trim(CAST(/* expression of unknown or erroneous type */ lu_month_rolling.parent_month_id as STRING)))
;
