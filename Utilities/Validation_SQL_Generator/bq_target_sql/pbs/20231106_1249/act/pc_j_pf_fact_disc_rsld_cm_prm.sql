-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pc_j_pf_fact_disc_rsld_cm_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', coalesce(a.count1, 0)), trim(CAST(coalesce(a.exp_gross_rbmt_crnt_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.exp_cont_alw_crnt_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.exp_payment_crnt_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.exp_charge_crnt_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.var_gross_rbmt_new_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.var_cont_alw_new_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.var_payment_new_amt, CAST(0 as BIGNUMERIC)) as STRING)), trim(CAST(coalesce(a.var_charge_new_amt, CAST(0 as BIGNUMERIC)) as STRING))) AS source_string
  FROM
    (
      SELECT
          count(*) AS count1,
          sum(fact_rcom_pars_discrepancy.exp_gross_rbmt_crnt_amt) AS exp_gross_rbmt_crnt_amt,
          sum(fact_rcom_pars_discrepancy.exp_cont_alw_crnt_amt) AS exp_cont_alw_crnt_amt,
          sum(fact_rcom_pars_discrepancy.exp_payment_crnt_amt) AS exp_payment_crnt_amt,
          sum(fact_rcom_pars_discrepancy.exp_charge_crnt_amt) AS exp_charge_crnt_amt,
          sum(fact_rcom_pars_discrepancy.var_gross_rbmt_new_amt) AS var_gross_rbmt_new_amt,
          sum(fact_rcom_pars_discrepancy.var_cont_alw_new_amt) AS var_cont_alw_new_amt,
          sum(fact_rcom_pars_discrepancy.var_payment_new_amt) AS var_payment_new_amt,
          sum(fact_rcom_pars_discrepancy.var_charge_new_amt) AS var_charge_new_amt
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs.fact_rcom_pars_discrepancy
        WHERE fact_rcom_pars_discrepancy.date_sid = CASE
           format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
          WHEN '' THEN 0
          ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) as INT64)
        END
         AND fact_rcom_pars_discrepancy.discrepancy_resolved_date <> DATE '0001-01-01'
         AND upper(fact_rcom_pars_discrepancy.coid) NOT IN(
          SELECT
              upper(parallon_client_detail.coid) AS coid
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs.parallon_client_detail
            WHERE upper(parallon_client_detail.company_code) = 'CHP'
        )
    ) AS a
;
