-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/hcaps_gl_stats_smry.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.hcaps_gl_stats_smry AS SELECT
    a.coid,
    a.company_code,
    a.gl_dept_num AS dept_num,
    a.gl_dept_num AS gl_dept_num,
    a.financial_service_code,
    CAST(NULL as STRING) AS gl_sub_account_num,
    a.pe_date,
    a.allocation_type_id,
    ROUND(a.gl_cm_cy_actual_amt, 3, 'ROUND_HALF_EVEN') AS cm_actual_amt,
    ROUND(a.gl_cm_cy_budget_amt, 3, 'ROUND_HALF_EVEN') AS cm_budget_amt,
    ROUND(a.gl_cm_py_actual_amt, 3, 'ROUND_HALF_EVEN') AS cm_prior_year_amt,
    ROUND(a.gl_qtd_cy_actual_amt, 3, 'ROUND_HALF_EVEN') AS qtd_actual_amt,
    ROUND(a.gl_qtd_cy_budget_amt, 3, 'ROUND_HALF_EVEN') AS qtd_budget_amt,
    ROUND(a.gl_qtd_py_actual_amt, 3, 'ROUND_HALF_EVEN') AS qtd_prior_year_amt,
    ROUND(a.gl_ytd_cy_actual_amt, 3, 'ROUND_HALF_EVEN') AS ytd_actual_amt,
    ROUND(a.gl_ytd_cy_budget_amt, 3, 'ROUND_HALF_EVEN') AS ytd_budget_amt,
    ROUND(a.gl_ytd_py_actual_amt, 3, 'ROUND_HALF_EVEN') AS ytd_prior_year_amt,
    a.data_source_code,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwps_base_views.gl_allocated_summary AS a
UNION ALL
SELECT
    a.coid,
    a.company_code,
    CASE
      WHEN a.gl_dept_num BETWEEN '000' AND '299' THEN '000'
      WHEN a.gl_dept_num BETWEEN '300' AND '499' THEN CAST(CASE
         a.gl_dept_num
        WHEN '' THEN 0
        ELSE CAST(a.gl_dept_num as INT64)
      END + 300 as STRING)
      WHEN a.gl_dept_num BETWEEN '500' AND '501'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN a.gl_dept_num BETWEEN '503' AND '508'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN a.gl_dept_num BETWEEN '540' AND '543'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN a.gl_dept_num BETWEEN '549' AND '550'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN upper(a.gl_dept_num) = '581'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN a.gl_dept_num BETWEEN '583' AND '589'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN upper(a.gl_dept_num) IN(
        '980', '981'
      )
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN upper(a.gl_dept_num) IN(
        '597', '980', '981'
      )
       AND a.gl_sub_account_num BETWEEN '600' AND '699' THEN a.gl_sub_account_num
      WHEN upper(a.gl_dept_num) = '597'
       AND a.gl_sub_account_num BETWEEN '000' AND '599' THEN concat('7', substr(a.gl_sub_account_num, 2, 2))
      WHEN upper(a.gl_dept_num) = '500'
       AND a.gl_sub_account_num BETWEEN '600' AND '699' THEN concat('7', substr(a.gl_sub_account_num, 2, 2))
      WHEN upper(a.gl_dept_num) = '507'
       AND a.gl_sub_account_num BETWEEN '600' AND '699' THEN concat('7', substr(a.gl_sub_account_num, 2, 2))
      WHEN upper(a.gl_dept_num) = '509'
       AND a.gl_sub_account_num BETWEEN '700' AND '799' THEN a.gl_sub_account_num
      WHEN a.gl_dept_num BETWEEN '500' AND '599' THEN '000'
      ELSE a.gl_dept_num
    END AS dept_num,
    a.gl_dept_num AS gl_dept_num,
    e.gl_account_rollup_group_code AS financial_service_code,
    substr(a.gl_sub_account_num, 1, 5) AS gl_sub_account_num,
    a.pe_date,
    CASE
      WHEN upper(a.gl_dept_num) LIKE '7%' THEN '1'
      ELSE '7'
    END AS allocation_type_id,
    ROUND(a.gl_cm_actual, 3, 'ROUND_HALF_EVEN') AS cm_actual_amt,
    ROUND(a.gl_cm_budget, 3, 'ROUND_HALF_EVEN') AS cm_budget_amt,
    ROUND(a.gl_cm_prior_year, 3, 'ROUND_HALF_EVEN') AS cm_prior_year_amt,
    ROUND(a.gl_qtd_actual, 3, 'ROUND_HALF_EVEN') AS qtd_actual_amt,
    ROUND(a.gl_qtd_budget, 3, 'ROUND_HALF_EVEN') AS qtd_budget_amt,
    ROUND(a.gl_qtd_prior_year, 3, 'ROUND_HALF_EVEN') AS qtd_prior_year_amt,
    ROUND(a.gl_ytd_actual, 3, 'ROUND_HALF_EVEN') AS ytd_actual_amt,
    ROUND(a.gl_ytd_budget, 3, 'ROUND_HALF_EVEN') AS ytd_budget_amt,
    ROUND(a.gl_ytd_prior_year, 3, 'ROUND_HALF_EVEN') AS ytd_prior_year_amt,
    a.data_source_code,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_summary AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwps_base_views.comast_nbt_supplement AS b ON upper(a.coid) = upper(b.coid)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_coa_gl_acct_rollup AS e ON upper(a.coid) = upper(e.coid)
     AND upper(a.gl_dept_num) = upper(e.gl_dept_num)
     AND upper(a.gl_sub_account_num) = upper(e.gl_sub_account_num)
     AND upper(e.gl_account_rollup_set_code) = 'FS'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwps_base_views.ref_stats_fscode AS f ON upper(e.gl_account_rollup_group_code) = upper(f.financial_service_code)
     AND upper(CASE
      WHEN upper(substr(e.gl_dept_num, 1, 1)) = '7'
       AND upper(e.gl_account_rollup_group_code) IN(
        '65050', '65100'
      ) THEN 'Y'
      ELSE 'N'
    END) = upper(f.dept_7xx_ind)
  WHERE a.pe_date NOT IN(
    SELECT
        b_0.pe_date
      FROM
        `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_summary_cm AS b_0
      GROUP BY 1
  )
UNION ALL
SELECT
    b.coid,
    b.company_code,
    CASE
      WHEN b.gl_dept_num BETWEEN '000' AND '299' THEN '000'
      WHEN b.gl_dept_num BETWEEN '300' AND '499' THEN CAST(CASE
         b.gl_dept_num
        WHEN '' THEN 0
        ELSE CAST(b.gl_dept_num as INT64)
      END + 300 as STRING)
      WHEN b.gl_dept_num BETWEEN '500' AND '501'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN b.gl_dept_num BETWEEN '503' AND '508'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN b.gl_dept_num BETWEEN '540' AND '543'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN b.gl_dept_num BETWEEN '549' AND '550'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN upper(b.gl_dept_num) = '581'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN b.gl_dept_num BETWEEN '583' AND '589'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN upper(b.gl_dept_num) IN(
        '980', '981'
      )
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN upper(b.gl_dept_num) IN(
        '597', '980', '981'
      )
       AND b.gl_sub_account_num BETWEEN '600' AND '699' THEN b.gl_sub_account_num
      WHEN upper(b.gl_dept_num) = '597'
       AND b.gl_sub_account_num BETWEEN '000' AND '599' THEN concat('7', substr(b.gl_sub_account_num, 2, 2))
      WHEN upper(b.gl_dept_num) = '500'
       AND b.gl_sub_account_num BETWEEN '600' AND '699' THEN concat('7', substr(b.gl_sub_account_num, 2, 2))
      WHEN upper(b.gl_dept_num) = '507'
       AND b.gl_sub_account_num BETWEEN '600' AND '699' THEN concat('7', substr(b.gl_sub_account_num, 2, 2))
      WHEN upper(b.gl_dept_num) = '509'
       AND b.gl_sub_account_num BETWEEN '700' AND '799' THEN b.gl_sub_account_num
      WHEN b.gl_dept_num BETWEEN '500' AND '599' THEN '000'
      ELSE b.gl_dept_num
    END AS dept_num,
    b.gl_dept_num AS gl_dept_num,
    e.gl_account_rollup_group_code AS financial_service_code,
    substr(b.gl_sub_account_num, 1, 5) AS gl_sub_account_num,
    b.pe_date,
    CASE
      WHEN upper(b.gl_dept_num) LIKE '7%' THEN '1'
      ELSE '7'
    END AS allocation_type_id,
    ROUND(b.gl_cm_actual, 3, 'ROUND_HALF_EVEN') AS cm_actual_amt,
    ROUND(b.gl_cm_budget, 3, 'ROUND_HALF_EVEN') AS cm_budget_amt,
    ROUND(b.gl_cm_prior_year, 3, 'ROUND_HALF_EVEN') AS cm_prior_year_amt,
    ROUND(b.gl_qtd_actual, 3, 'ROUND_HALF_EVEN') AS qtd_actual_amt,
    ROUND(b.gl_qtd_budget, 3, 'ROUND_HALF_EVEN') AS qtd_budget_amt,
    ROUND(b.gl_qtd_prior_year, 3, 'ROUND_HALF_EVEN') AS qtd_prior_year_amt,
    ROUND(b.gl_ytd_actual, 3, 'ROUND_HALF_EVEN') AS ytd_actual_amt,
    ROUND(b.gl_ytd_budget, 3, 'ROUND_HALF_EVEN') AS ytd_budget_amt,
    ROUND(b.gl_ytd_prior_year, 3, 'ROUND_HALF_EVEN') AS ytd_prior_year_amt,
    b.data_source_code,
    b.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_summary_cm AS b
    INNER JOIN `hca-hin-dev-cur-parallon`.edwps_base_views.comast_nbt_supplement AS d ON upper(b.coid) = upper(d.coid)
    INNER JOIN `hca-hin-dev-cur-parallon`.edwfs_base_views.gl_coa_gl_acct_rollup AS e ON upper(b.coid) = upper(e.coid)
     AND upper(b.gl_dept_num) = upper(e.gl_dept_num)
     AND upper(b.gl_sub_account_num) = upper(e.gl_sub_account_num)
     AND upper(e.gl_account_rollup_set_code) = 'FS'
    INNER JOIN `hca-hin-dev-cur-parallon`.edwps_base_views.ref_stats_fscode AS f ON upper(e.gl_account_rollup_group_code) = upper(f.financial_service_code)
     AND upper(CASE
      WHEN upper(substr(e.gl_dept_num, 1, 1)) = '7'
       AND upper(e.gl_account_rollup_group_code) IN(
        '65050', '65100'
      ) THEN 'Y'
      ELSE 'N'
    END) = upper(f.dept_7xx_ind)
UNION ALL
SELECT
    a.coid,
    a.company_code,
    a.dept_num AS dept_num,
    a.dept_num AS gl_dept_num,
    a.stat_code AS financial_service_code,
    CAST(NULL as STRING) AS gl_sub_account_num,
    a.pe_date,
    CASE
      WHEN upper(a.dept_num) LIKE '7%' THEN '1'
      ELSE '7'
    END AS allocation_type_id,
    ROUND(ROUND(a.stat_cm_actual, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS cm_actual_amt,
    ROUND(ROUND(a.stat_cm_budget, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS cm_budget_amt,
    ROUND(ROUND(a.stat_cm_prior_year, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS cm_prior_year_amt,
    ROUND(ROUND(a.stat_qtd_actual, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS qtd_actual_amt,
    ROUND(ROUND(a.stat_qtd_budget, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS qtd_budget_amt,
    ROUND(ROUND(a.stat_qtd_prior_year, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS qtd_prior_year_amt,
    ROUND(ROUND(a.stat_ytd_actual, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS ytd_actual_amt,
    ROUND(ROUND(a.stat_ytd_budget, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS ytd_budget_amt,
    ROUND(ROUND(a.stat_ytd_prior_year, 4, 'ROUND_HALF_EVEN'), 3, 'ROUND_HALF_EVEN') AS ytd_prior_year_amt,
    a.data_source_code,
    a.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwfs_base_views.stats_zero AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwps_base_views.comast_nbt_supplement AS c ON upper(a.coid) = upper(c.coid)
;
