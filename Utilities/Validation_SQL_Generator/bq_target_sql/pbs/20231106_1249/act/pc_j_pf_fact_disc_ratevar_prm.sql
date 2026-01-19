-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pc_j_pf_fact_disc_ratevar_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

-- Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access
SELECT
    concat(coalesce(CAST(sum(disc.exp_gross_rbmt_rate_amt) as STRING), '0'), coalesce(CAST(sum(disc.exp_cont_alw_rate_amt) as STRING), '0'), coalesce(CAST(sum(disc.exp_payment_rate_amt) as STRING), '0')) AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_pars_discrepancy AS disc
  WHERE disc.patient_sid = NUMERIC '999999999999'
   AND disc.date_sid = CASE
     format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
    WHEN '' THEN 0.0
    ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) as FLOAT64)
  END
   AND upper(disc.coid) NOT IN(
    SELECT
        upper(parallon_client_detail.coid) AS coid
      FROM
        `hca-hin-dev-cur-parallon`.edwpbs.parallon_client_detail
      WHERE upper(parallon_client_detail.company_code) = 'CHP'
  )
;
