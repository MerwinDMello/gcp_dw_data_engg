-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END = 1
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END IN(
    8, 9, 10, 11, 12, 1
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END = 2
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END IN(
    9, 10, 11, 12, 1, 2
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END = 3
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END IN(
    10, 11, 12, 1, 2, 3
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END = 4
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END IN(
    11, 12, 1, 2, 3, 4
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END = 5
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END IN(
    12, 1, 2, 3, 4, 5
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
  WHERE CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END >= 6
   AND CASE
     substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.child_month_id)), 5, 2) as FLOAT64)
  END >= CASE
     substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2)
    WHEN '' THEN 0.0
    ELSE CAST(substr(trim(format('%11d', lu_month_rolling.parent_month_id)), 5, 2) as FLOAT64)
  END - 5
   AND upper(trim(CAST(lu_month_rolling.child_month_id as STRING))) = upper(trim(CAST(lu_month_rolling.parent_month_id as STRING)))
;
