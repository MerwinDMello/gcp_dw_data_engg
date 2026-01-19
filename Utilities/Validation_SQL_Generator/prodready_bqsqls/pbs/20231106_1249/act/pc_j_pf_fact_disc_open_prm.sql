-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/pc_j_pf_fact_disc_open_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- Locking table edwpbs.Fact_RCOM_PARS_Discrepancy for access

SELECT concat('0', trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_gross_rbmt_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_cont_alw_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_payment_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_charge_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING))) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_pars_discrepancy
WHERE fact_rcom_pars_discrepancy.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                WHEN '' THEN 0
                                                ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                                            END
  AND fact_rcom_pars_discrepancy.discrepancy_resolved_date = DATE '0001-01-01'
  AND fact_rcom_pars_discrepancy.patient_sid <> NUMERIC '999999999999'
  AND upper(fact_rcom_pars_discrepancy.coid) NOT IN
    (SELECT upper(parallon_client_detail.coid) AS coid
     FROM {{ params.param_pbs_core_dataset_name }}.parallon_client_detail
     WHERE upper(parallon_client_detail.company_code) = 'CHP' )