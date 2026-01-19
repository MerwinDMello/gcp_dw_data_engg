-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_dmr_metric_report.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_report AS SELECT
    fact_dmr_metric_all_lvl.service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    fact_dmr_metric_all_lvl.parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    fact_dmr_metric_all_lvl.dmr_day_month_ind,
    fact_dmr_metric_all_lvl.dmr_metric_code,
    fact_dmr_metric_all_lvl.dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    fact_dmr_metric_all_lvl.dmr_fin_class_code,
    fact_dmr_metric_all_lvl.dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE fact_dmr_metric_all_lvl.dmr_metric_code NOT IN(
    '9A', '9B', '29A'
  )
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '4' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '1'
     AND upper(b.dmr_metric_code) = '3'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '5' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    a.dmr_metric_value / extract(DAY from CAST(a.rptg_date as DATETIME)) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
  WHERE upper(a.dmr_metric_code) = '1'
   AND a.dmr_day_month_ind IN(
    'DCMTH', 'DPMTH'
  )
   AND a.dmr_patient_type_code IN(
    'AP', 'NB'
  )
   AND upper(a.dmr_fin_class_code) = 'NA'
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '5' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    a.dmr_metric_value / extract(DAY from CAST(a.rptg_date as DATETIME)) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
  WHERE upper(a.dmr_metric_code) = '27'
   AND upper(a.dmr_day_month_ind) = 'DCMTH'
   AND a.dmr_patient_type_code IN(
    'AP', 'NB'
  )
   AND upper(a.dmr_fin_class_code) <> 'NA'
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '5' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    a.dmr_metric_value / b.day_of_year AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.lu_date AS b ON upper(a.dmr_metric_code) = '1'
     AND a.rptg_date = b.date_id
     AND upper(a.dmr_day_month_ind) = 'DCYTD'
     AND a.dmr_patient_type_code NOT IN(
      'ACS', 'NST'
    )
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '6' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN a.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE b.dmr_metric_value / a.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '1'
     AND upper(b.dmr_metric_code) = '1A'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_patient_type_code IN(
      'AP', 'NB'
    )
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '35' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '10'
     AND upper(b.dmr_metric_code) = '29A'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '35' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '10'
     AND upper(b.dmr_metric_code) = '27'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_patient_type_code IN(
      'AP', 'NB'
    )
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
     AND upper(a.dmr_day_month_ind) <> 'DCDAT'
UNION ALL
SELECT
    max(a.service_type_name) AS service_type_name,
    a.fact_lvl_code,
    max(a.parent_code) AS parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '29B' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    sum(a.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
  WHERE upper(a.dmr_metric_code) = '29B'
  GROUP BY upper(a.service_type_name), 2, upper(a.parent_code), 4, 5, 7, 8
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '30' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '27B' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '27'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '27'
     AND upper(b.dmr_metric_code) = '27B'
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    fact_dmr_metric_all_lvl.dmr_day_month_ind,
    '30' AS dmr_metric_code,
    fact_dmr_metric_all_lvl.dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    CASE
      WHEN sum(fact_dmr_metric_all_lvl.dmr_metric_value) = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE sum(fact_dmr_metric_all_lvl.dmr_metric_value) / sum(fact_dmr_metric_all_lvl.dmr_metric_value)
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '27'
   AND upper(fact_dmr_metric_all_lvl.dmr_code) = 'NA'
   AND fact_dmr_metric_all_lvl.dmr_patient_type_code IN(
    'AP', 'NB'
  )
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '30' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      WHEN a.dmr_metric_value < 0
       AND b.dmr_metric_value < 0 THEN a.dmr_metric_value / b.dmr_metric_value * -1
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '16B' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '16'
         AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) <> 'NA'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '16'
     AND upper(b.dmr_metric_code) = '16B'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND upper(a.dmr_fin_class_code) <> 'NA'
     AND upper(a.dmr_code) = 'NA'
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    CASE
      WHEN upper(fact_dmr_metric_all_lvl.dmr_day_month_ind) = 'DCMTH' THEN 'DCMTD'
      ELSE fact_dmr_metric_all_lvl.dmr_day_month_ind
    END AS dmr_day_month_ind,
    '30' AS dmr_metric_code,
    fact_dmr_metric_all_lvl.dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    CASE
      WHEN sum(fact_dmr_metric_all_lvl.dmr_metric_value) = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE sum(fact_dmr_metric_all_lvl.dmr_metric_value) / sum(fact_dmr_metric_all_lvl.dmr_metric_value)
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '16'
   AND upper(fact_dmr_metric_all_lvl.dmr_patient_type_code) = 'O'
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, fact_dmr_metric_all_lvl.dmr_day_month_ind, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '32' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      WHEN a.dmr_metric_value < 0
       AND b.dmr_metric_value < 0 THEN a.dmr_metric_value / b.dmr_metric_value * -1
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          fact_dmr_metric_all_lvl.dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          fact_dmr_metric_all_lvl.dmr_fin_class_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '10'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8, 9
    ) AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          fact_dmr_metric_all_lvl.dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          fact_dmr_metric_all_lvl.dmr_fin_class_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '16'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8, 9
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '10'
     AND upper(b.dmr_metric_code) = '16'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
     AND upper(a.dmr_code) = 'NA'
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '32' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code AS dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      WHEN a.dmr_metric_value < 0
       AND b.dmr_metric_value < 0 THEN a.dmr_metric_value / b.dmr_metric_value * -1
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          fact_dmr_metric_all_lvl.dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          'NA' AS dmr_fin_class_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '10'
         AND upper(fact_dmr_metric_all_lvl.dmr_patient_type_code) = 'O'
         AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) <> 'NA'
         AND upper(fact_dmr_metric_all_lvl.dmr_code) = 'NA'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8
    ) AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          fact_dmr_metric_all_lvl.dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          'NA' AS dmr_fin_class_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '16'
         AND upper(fact_dmr_metric_all_lvl.dmr_patient_type_code) = 'O'
         AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) <> 'NA'
         AND upper(fact_dmr_metric_all_lvl.dmr_code) = 'NA'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '10'
     AND upper(b.dmr_metric_code) = '16'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND upper(a.dmr_fin_class_code) = upper(b.dmr_fin_class_code)
     AND upper(a.dmr_code) = 'NA'
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    fact_dmr_metric_all_lvl.dmr_day_month_ind,
    fact_dmr_metric_all_lvl.dmr_metric_code,
    'NA' AS dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE fact_dmr_metric_all_lvl.dmr_metric_code IN(
    '10', '28'
  )
   AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) <> 'NA'
   AND fact_dmr_metric_all_lvl.dmr_patient_type_code IN(
    'AP', 'NB'
  )
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, fact_dmr_metric_all_lvl.dmr_code, 8
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    fact_dmr_metric_all_lvl.dmr_day_month_ind,
    fact_dmr_metric_all_lvl.dmr_metric_code,
    'NA' AS dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE fact_dmr_metric_all_lvl.dmr_metric_code IN(
    '16', '17'
  )
   AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) <> 'NA'
   AND upper(fact_dmr_metric_all_lvl.dmr_patient_type_code) = 'O'
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, fact_dmr_metric_all_lvl.dmr_code, 8
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    CASE
      WHEN upper(fact_dmr_metric_all_lvl.dmr_day_month_ind) = 'DCMTD' THEN 'DCMTH'
      ELSE fact_dmr_metric_all_lvl.dmr_day_month_ind
    END AS dmr_day_month_ind,
    '36' AS dmr_metric_code,
    'NA' AS dmr_code,
    'I' AS dmr_patient_type_code,
    fact_dmr_metric_all_lvl.dmr_fin_class_code,
    sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE fact_dmr_metric_all_lvl.dmr_metric_code IN(
    '23', '10'
  )
   AND fact_dmr_metric_all_lvl.dmr_patient_type_code IN(
    'I', 'ACS'
  )
   AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) = 'NA'
   AND fact_dmr_metric_all_lvl.dmr_code IN(
    'NA', '100'
  )
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8, 9
UNION ALL
SELECT
    max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    CASE
      WHEN upper(fact_dmr_metric_all_lvl.dmr_day_month_ind) = 'DCMTD' THEN 'DCMTH'
      ELSE fact_dmr_metric_all_lvl.dmr_day_month_ind
    END AS dmr_day_month_ind,
    '37' AS dmr_metric_code,
    fact_dmr_metric_all_lvl.dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    fact_dmr_metric_all_lvl.dmr_fin_class_code,
    sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE fact_dmr_metric_all_lvl.dmr_metric_code IN(
    '23', '24', '10'
  )
   AND upper(fact_dmr_metric_all_lvl.dmr_patient_type_code) = 'O'
   AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) = 'NA'
   AND upper(fact_dmr_metric_all_lvl.dmr_code) = 'NA'
  GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, 6, 7, 8, 9
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '29' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '33'
     AND upper(b.dmr_metric_code) = '34'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '9' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '9A'
     AND upper(b.dmr_metric_code) = '9B'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '29' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_day_month_ind) <> 'DCDAT'
     AND upper(a.dmr_metric_code) = '1'
     AND upper(b.dmr_metric_code) = '7'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_patient_type_code IN(
      'AP', 'NB'
    )
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '29' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND upper(a.dmr_day_month_ind) = 'DCMTH'
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '27'
     AND upper(b.dmr_metric_code) = '28'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '29' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    a.dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '29A'
     AND upper(b.dmr_metric_code) = '29B'
     AND a.dmr_code = b.dmr_code
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
     AND a.dmr_fin_class_code = b.dmr_fin_class_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '9' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    CASE
      WHEN b.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE a.dmr_metric_value / b.dmr_metric_value
    END AS dmr_metric_value
  FROM
    (
      SELECT
          max(a_0.service_type_name) AS service_type_name,
          a_0.fact_lvl_code,
          max(a_0.parent_code) AS parent_code,
          a_0.rptg_date,
          a_0.dmr_day_month_ind,
          '9A' AS dmr_metric_cd,
          a_0.dmr_code,
          a_0.dmr_patient_type_code,
          sum(a_0.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a_0
        WHERE upper(a_0.dmr_metric_code) = '9A'
        GROUP BY upper(a_0.service_type_name), 2, upper(a_0.parent_code), 4, 5, 7, 8
    ) AS a
    CROSS JOIN (
      SELECT
          max(a_0.service_type_name) AS service_type_name,
          a_0.fact_lvl_code,
          max(a_0.parent_code) AS parent_code,
          a_0.rptg_date,
          a_0.dmr_day_month_ind,
          '9B' AS dmr_metric_cd,
          a_0.dmr_code,
          a_0.dmr_patient_type_code,
          sum(a_0.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl AS a_0
        WHERE upper(a_0.dmr_metric_code) = '9B'
        GROUP BY upper(a_0.service_type_name), 2, upper(a_0.parent_code), 4, 5, 7, 8
    ) AS b
  WHERE upper(a.service_type_name) = upper(b.service_type_name)
   AND a.fact_lvl_code = b.fact_lvl_code
   AND upper(a.parent_code) = upper(b.parent_code)
   AND a.rptg_date = b.rptg_date
   AND a.dmr_day_month_ind = b.dmr_day_month_ind
   AND a.dmr_patient_type_code = b.dmr_patient_type_code
   AND a.dmr_code = b.dmr_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '35' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    CASE
      WHEN a.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE b.dmr_metric_value / a.dmr_metric_value
    END AS dmr_metric_value
  FROM
    (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '29A' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '29A'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '10' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '10'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '29A'
     AND upper(b.dmr_metric_code) = '10'
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
UNION ALL
SELECT
    a.service_type_name,
    a.fact_lvl_code,
    a.parent_code,
    a.rptg_date,
    a.dmr_day_month_ind,
    '35' AS dmr_metric_code,
    a.dmr_code,
    a.dmr_patient_type_code,
    'NA' AS dmr_fin_class_code,
    CASE
      WHEN a.dmr_metric_value = 0 THEN CAST(0 as BIGNUMERIC)
      ELSE b.dmr_metric_value / a.dmr_metric_value
    END AS dmr_metric_value
  FROM
    (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '27' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '27'
         AND upper(fact_dmr_metric_all_lvl.dmr_day_month_ind) <> 'DCDAT'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS a
    INNER JOIN (
      SELECT
          max(fact_dmr_metric_all_lvl.service_type_name) AS service_type_name,
          fact_dmr_metric_all_lvl.fact_lvl_code,
          max(fact_dmr_metric_all_lvl.parent_code) AS parent_code,
          fact_dmr_metric_all_lvl.rptg_date,
          fact_dmr_metric_all_lvl.dmr_day_month_ind,
          '10' AS dmr_metric_code,
          fact_dmr_metric_all_lvl.dmr_code,
          fact_dmr_metric_all_lvl.dmr_patient_type_code,
          sum(fact_dmr_metric_all_lvl.dmr_metric_value) AS dmr_metric_value
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
        WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '10'
        GROUP BY upper(fact_dmr_metric_all_lvl.service_type_name), 2, upper(fact_dmr_metric_all_lvl.parent_code), 4, 5, fact_dmr_metric_all_lvl.dmr_metric_code, 7, 8
    ) AS b ON upper(a.service_type_name) = upper(b.service_type_name)
     AND a.fact_lvl_code = b.fact_lvl_code
     AND upper(a.parent_code) = upper(b.parent_code)
     AND a.rptg_date = b.rptg_date
     AND a.dmr_day_month_ind = b.dmr_day_month_ind
     AND upper(a.dmr_metric_code) = '27'
     AND upper(b.dmr_metric_code) = '10'
     AND a.dmr_patient_type_code = b.dmr_patient_type_code
UNION ALL
SELECT
    fact_dmr_metric_all_lvl.service_type_name,
    fact_dmr_metric_all_lvl.fact_lvl_code,
    fact_dmr_metric_all_lvl.parent_code,
    fact_dmr_metric_all_lvl.rptg_date,
    fact_dmr_metric_all_lvl.dmr_day_month_ind,
    '27' AS dmr_metric_code,
    fact_dmr_metric_all_lvl.dmr_code,
    fact_dmr_metric_all_lvl.dmr_patient_type_code,
    fact_dmr_metric_all_lvl.dmr_fin_class_code,
    fact_dmr_metric_all_lvl.dmr_metric_value
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_views.fact_dmr_metric_all_lvl
  WHERE upper(fact_dmr_metric_all_lvl.dmr_metric_code) = '1'
   AND upper(fact_dmr_metric_all_lvl.dmr_code) = 'NA'
   AND upper(fact_dmr_metric_all_lvl.dmr_fin_class_code) = 'NA'
;
