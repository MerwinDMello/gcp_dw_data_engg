-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pa_j_pf_rpt_ar_ada_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(coalesce(trim(CAST(sum(src.sum1) as STRING)), '0'), coalesce(trim(CAST(sum(src.sum2) as STRING)), '0'), coalesce(trim(CAST(sum(src.sum3) as STRING)), '0')) AS source_string
  FROM
    (
      SELECT
          sum(x.bad_debt_writeoff_amt) AS sum1,
          sum(x.non_secn_self_pay_ar_amt) AS sum2,
          sum(x.non_secn_unins_disc_amt) AS sum3
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs.rpt_ar_ada_detail AS x
        WHERE x.month_id = CASE
           format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
          WHEN '' THEN 0.0
          ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) as FLOAT64)
        END
    ) AS src
;
