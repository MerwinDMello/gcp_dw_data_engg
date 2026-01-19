DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/reimbursement_stats_load.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Pritam Tawale
          Date: 12/07/2018
          Name: Reimbursement_Stats_Load.sql
          Mod1: Loads data into reimbursement_stats_load table collects data from reimbursement_sicrepancy.
**************************************************************************************************/ BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_stats AS mt USING
  (SELECT DISTINCT max(b.coid) AS coid,
                   CAST(ROUND(coalesce(sum(b.overpayment), CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS overpayment,
                   CAST(ROUND(coalesce(sum(b.underpayment), CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS underpayment,
                   CAST(ROUND(coalesce(sum(b.non_financial), CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS non_financial,
                   coalesce(sum(b.overpayment_count), 0) AS overpayment_count,
                   coalesce(sum(b.underpayment_count), 0) AS underpayment_count,
                   coalesce(sum(b.non_financial_count), 0) AS non_financial_count,
                   coalesce(sum(b.overpayment_count + b.underpayment_count + b.non_financial_count), 0) AS total_discrepancies,
                   b.dw_last_update_date_time
   FROM
     (SELECT a.coid,
             a.dw_last_update_date_time,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) < 0 THEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC))
             END AS overpayment,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) > 0 THEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC))
             END AS underpayment,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) = 0 THEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC))
             END AS non_financial,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) < 0 THEN 1
                 ELSE 0
             END AS overpayment_count,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) > 0 THEN 1
                 ELSE 0
             END AS underpayment_count,
             CASE
                 WHEN coalesce(a.var_gross_reimbursement_amt, CAST(0 AS NUMERIC)) = 0 THEN 1
                 ELSE 0
             END AS non_financial_count
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy AS a
      WHERE DATE(a.dw_last_update_date_time) = DATE(
                                                      (SELECT max(cc_reimbursement_discrepancy.dw_last_update_date_time)
                                                       FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy)) ) AS b
   GROUP BY upper(b.coid),
            9) AS ms ON upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1'))
AND (coalesce(mt.overpayment, NUMERIC '0') = coalesce(ms.overpayment, NUMERIC '0')
     AND coalesce(mt.overpayment, NUMERIC '1') = coalesce(ms.overpayment, NUMERIC '1'))
AND (coalesce(mt.underpayment, NUMERIC '0') = coalesce(ms.underpayment, NUMERIC '0')
     AND coalesce(mt.underpayment, NUMERIC '1') = coalesce(ms.underpayment, NUMERIC '1'))
AND (coalesce(mt.non_financial, NUMERIC '0') = coalesce(ms.non_financial, NUMERIC '0')
     AND coalesce(mt.non_financial, NUMERIC '1') = coalesce(ms.non_financial, NUMERIC '1'))
AND (coalesce(mt.overpayment_count, 0) = coalesce(ms.overpayment_count, 0)
     AND coalesce(mt.overpayment_count, 1) = coalesce(ms.overpayment_count, 1))
AND (coalesce(mt.underpayment_count, 0) = coalesce(ms.underpayment_count, 0)
     AND coalesce(mt.underpayment_count, 1) = coalesce(ms.underpayment_count, 1))
AND (coalesce(mt.non_financial_count, 0) = coalesce(ms.non_financial_count, 0)
     AND coalesce(mt.non_financial_count, 1) = coalesce(ms.non_financial_count, 1))
AND (coalesce(mt.total_discrepancies, 0) = coalesce(ms.total_discrepancies, 0)
     AND coalesce(mt.total_discrepancies, 1) = coalesce(ms.total_discrepancies, 1))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (coid,
        overpayment,
        underpayment,
        non_financial,
        overpayment_count,
        underpayment_count,
        non_financial_count,
        total_discrepancies,
        dw_last_update_date_time)
VALUES (ms.coid, ms.overpayment, ms.underpayment, ms.non_financial, ms.overpayment_count, ms.underpayment_count, ms.non_financial_count, ms.total_discrepancies, ms.dw_last_update_date_time);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;