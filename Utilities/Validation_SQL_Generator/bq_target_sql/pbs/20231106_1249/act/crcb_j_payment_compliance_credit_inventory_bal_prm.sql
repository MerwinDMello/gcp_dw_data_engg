-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/act/crcb_j_payment_compliance_credit_inventory_bal_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT
    concat(format('%20d', coalesce(count(*), 0)), CAST(coalesce(sum(cbd.refund_amt), NUMERIC '0') as STRING), CAST(coalesce(sum(cbd.total_account_balance_amt), NUMERIC '0') as STRING)) AS source_string
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payment_compliance_credit_inventory AS cbd
  WHERE cbd.reporting_date = current_date('US/Central')
   AND upper(cbd.credit_balance_refund_ind) = 'B'
;
