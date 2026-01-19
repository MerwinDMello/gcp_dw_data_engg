-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_date_dim_vw.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_date_dim_vw AS SELECT DISTINCT
    eis_date_dim.time_id AS time_id,
    'Months' AS time_gen02,
    'All_Months' AS time_gen03,
    eis_date_dim.time_member,
    eis_date_dim.time_id AS time_sort2,
    eis_date_dim.time_id AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt001_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt001_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt001_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt001_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt001_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt001_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt002_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt002_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt002_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt002_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt002_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt002_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt003_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt003_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt003_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt003_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt003_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt003_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt004_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt004_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt004_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt004_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt004_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt004_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt005_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt005_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt005_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt005_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt005_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt005_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt006_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt006_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt006_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt006_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt006_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt006_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt007_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt007_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt007_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt007_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt007_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt007_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt008_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt008_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt008_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt008_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt008_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt008_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt009_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt009_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt009_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt009_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt009_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt009_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt010_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt010_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt010_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt010_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt010_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt010_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt011_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt011_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt011_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt011_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt011_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt011_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
UNION ALL
SELECT
    1000000 + eis_date_dim.time_id AS time_id,
    substr(eis_date_dim.alt012_gen02, 1, 50) AS time_gen02,
    eis_date_dim.alt012_gen03 AS time_gen03,
    eis_date_dim.time_member,
    1000000 + eis_date_dim.time_id AS time_sort2,
    1000000 + CASE
       upper(substr(eis_date_dim.alt012_gen03, 5, 3))
      WHEN 'JAN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '01')) as INT64)
      WHEN 'FEB' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '02')) as INT64)
      WHEN 'MAR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '03')) as INT64)
      WHEN 'APR' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '04')) as INT64)
      WHEN 'MAY' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '05')) as INT64)
      WHEN 'JUN' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '06')) as INT64)
      WHEN 'JUL' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '07')) as INT64)
      WHEN 'AUG' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '08')) as INT64)
      WHEN 'SEP' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '09')) as INT64)
      WHEN 'OCT' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '10')) as INT64)
      WHEN 'NOV' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '11')) as INT64)
      WHEN 'DEC' THEN CAST(bqutil.fn.cw_td_normalize_number(concat(substr(substr(eis_date_dim.alt012_gen03, 9, 4), 1, 4), '12')) as INT64)
      ELSE 0
    END AS time_sort1,
    eis_date_dim.time_member_info_months,
    eis_date_dim.time_uda,
    eis_date_dim.time_uda2,
    eis_date_dim.time_uda3,
    eis_date_dim.time_uda4,
    eis_date_dim.time_uda5,
    eis_date_dim.time_uda6,
    eis_date_dim.time_uda7,
    eis_date_dim.last_13_mth_flag,
    eis_date_dim.last24_flag
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim
  WHERE upper(eis_date_dim.alt012_gen02) <> ' '
   AND upper(substr(eis_date_dim.alt012_gen03, 5, 8)) IN(
    SELECT DISTINCT
        upper(substr(eis_date_dim_0.alt001_gen03, 5, 8))
      FROM
        `hca-hin-dev-cur-parallon`.edwpf_base_views.eis_date_dim AS eis_date_dim_0
      WHERE eis_date_dim_0.current_mth = 'Y'
  )
   AND eis_date_dim.current_mth = 'Y'
;
