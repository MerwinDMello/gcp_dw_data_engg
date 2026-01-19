-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_day_interval.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_day_interval AS SELECT
    d9.*
  FROM
    (
      SELECT
          '99' AS day_interval_sid,
          '8.00 PM ' AS day_interval_desc,
          '99' AS day_interval_sort_order
    ) AS d9
UNION DISTINCT
SELECT
    substr(d1.day_interval_sid, 1, 2) AS day_interval_sid,
    d1.day_interval_desc,
    substr(d1.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '0' AS day_interval_sid,
          '4.00 AM ' AS day_interval_desc,
          '0' AS day_interval_sort_order
    ) AS d1
UNION DISTINCT
SELECT
    substr(d2.day_interval_sid, 1, 2) AS day_interval_sid,
    substr(d2.day_interval_desc, 1, 8) AS day_interval_desc,
    substr(d2.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '1' AS day_interval_sid,
          '6.00 AM' AS day_interval_desc,
          '1' AS day_interval_sort_order
    ) AS d2
UNION DISTINCT
SELECT
    substr(d3.day_interval_sid, 1, 2) AS day_interval_sid,
    d3.day_interval_desc,
    substr(d3.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '2' AS day_interval_sid,
          '8.00 AM ' AS day_interval_desc,
          '2' AS day_interval_sort_order
    ) AS d3
UNION DISTINCT
SELECT
    substr(d4.day_interval_sid, 1, 2) AS day_interval_sid,
    substr(d4.day_interval_desc, 1, 8) AS day_interval_desc,
    substr(d4.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '3' AS day_interval_sid,
          '10.00 AM ' AS day_interval_desc,
          '3' AS day_interval_sort_order
    ) AS d4
UNION DISTINCT
SELECT
    substr(d5.day_interval_sid, 1, 2) AS day_interval_sid,
    substr(d5.day_interval_desc, 1, 8) AS day_interval_desc,
    substr(d5.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '4' AS day_interval_sid,
          '12.00 PM ' AS day_interval_desc,
          '4' AS day_interval_sort_order
    ) AS d5
UNION DISTINCT
SELECT
    substr(d6.day_interval_sid, 1, 2) AS day_interval_sid,
    substr(d6.day_interval_desc, 1, 8) AS day_interval_desc,
    substr(d6.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '5' AS day_interval_sid,
          '2.00 PM' AS day_interval_desc,
          '5' AS day_interval_sort_order
    ) AS d6
UNION DISTINCT
SELECT
    substr(d7.day_interval_sid, 1, 2) AS day_interval_sid,
    d7.day_interval_desc,
    substr(d7.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '6' AS day_interval_sid,
          '4.00 PM ' AS day_interval_desc,
          '6' AS day_interval_sort_order
    ) AS d7
UNION DISTINCT
SELECT
    substr(d8.day_interval_sid, 1, 2) AS day_interval_sid,
    d8.day_interval_desc,
    substr(d8.day_interval_sort_order, 1, 2) AS day_interval_sort_order
  FROM
    (
      SELECT
          '7' AS day_interval_sid,
          '6.00 PM ' AS day_interval_desc,
          '7' AS day_interval_sort_order
    ) AS d8
;
