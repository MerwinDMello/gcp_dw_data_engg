##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = ''; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_pbs_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(trim(regexp_replace(format('%#17.2f', CAST(ROUND(sum(k.transaction_amt), 2, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(CAST(sum(k.non_sec_self_pay_ar_amt) AS STRING)), trim(CAST(sum(k.non_sec_uninsured_discount) AS STRING))) AS source_string
FROM
  (SELECT in_query.eom_end,
          sum(in_query.non_sec_self_pay_ar_amt) AS non_sec_self_pay_ar_amt,
          sum(in_query.non_sec_uninsured_discount) AS non_sec_uninsured_discount,
          sum(in_query.transaction_amt) AS transaction_amt
   FROM
     (SELECT eom_end AS eom_end,
             sum(CASE
                     WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA')
                          AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                          AND pem.total_account_balance_amt > 0 THEN pem.patient_balance_amt
                     ELSE CAST(0 AS NUMERIC)
                 END) + sum(CASE
                                WHEN (upper(format_date('%Y%m', pem.discharge_date)) > upper(pem.rptg_period)
                                      OR upper(pem.account_status_code) = 'UB'
                                      AND pem.discharge_date IS NOT NULL
                                      AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                      AND pem.total_account_balance_amt <> 0)
                                     AND pem.financial_class_code = 99 THEN pem.total_account_balance_amt
                                ELSE CAST(0 AS NUMERIC)
                            END) + sum(CASE
                                           WHEN pem.financial_class_code = 99
                                                AND (upper(pem.account_status_code) = 'UB'
                                                     AND upper(substr(pem.patient_type_code, 2, 1)) <> 'P'
                                                     AND pem.discharge_date IS NULL
                                                     AND pem.total_account_balance_amt <> 0
                                                     OR upper(pem.account_status_code) = 'UB'
                                                     AND upper(pem.unbill_pre_admit_ind) = 'Y') THEN pem.total_account_balance_amt
                                           ELSE CAST(0 AS NUMERIC)
                                       END) + sum(CASE
                                                      WHEN pem.financial_class_code = 15
                                                           AND (upper(format_date('%Y%m', pem.discharge_date)) > upper(pem.rptg_period)
                                                                OR upper(pem.account_status_code) = 'UB'
                                                                AND pem.discharge_date IS NOT NULL
                                                                AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                AND pem.total_account_balance_amt <> 0) THEN pem.total_account_balance_amt
                                                      ELSE CAST(0 AS NUMERIC)
                                                  END) + sum(CASE
                                                                 WHEN (upper(pem.account_status_code) = 'UB'
                                                                       AND upper(substr(pem.patient_type_code, 2, 1)) <> 'P'
                                                                       AND pem.discharge_date IS NULL
                                                                       AND pem.total_account_balance_amt <> 0
                                                                       OR upper(pem.account_status_code) = 'UB'
                                                                       AND upper(pem.unbill_pre_admit_ind) = 'Y')
                                                                      AND pem.financial_class_code = 15 THEN pem.total_account_balance_amt
                                                                 ELSE CAST(0 AS NUMERIC)
                                                             END) + sum(CASE
                                                                            WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                 AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                 AND pem.payor_balance_amt_ins1 > 0
                                                                                 AND pem.financial_class_code_ins1 = 15 THEN pem.payor_balance_amt_ins1
                                                                            ELSE CAST(0 AS NUMERIC)
                                                                        END) + sum(CASE
                                                                                       WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                            AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                            AND pem.payor_balance_amt_ins2 > 0
                                                                                            AND pem.financial_class_code_ins2 = 15 THEN pem.payor_balance_amt_ins2
                                                                                       ELSE CAST(0 AS NUMERIC)
                                                                                   END) + sum(CASE
                                                                                                  WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                       AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                                       AND pem.payor_balance_amt_ins3 > 0
                                                                                                       AND pem.financial_class_code_ins3 = 15 THEN pem.payor_balance_amt_ins3
                                                                                                  ELSE CAST(0 AS NUMERIC)
                                                                                              END) + sum(CASE
                                                                                                             WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                  AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                                                  AND pem.payor_balance_amt_ins1 > 0
                                                                                                                  AND pem.financial_class_code_ins1 = 99 THEN pem.payor_balance_amt_ins1
                                                                                                             ELSE CAST(0 AS NUMERIC)
                                                                                                         END) + sum(CASE
                                                                                                                        WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                             AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                                                             AND pem.payor_balance_amt_ins2 > 0
                                                                                                                             AND pem.financial_class_code_ins2 = 99 THEN pem.payor_balance_amt_ins2
                                                                                                                        ELSE CAST(0 AS NUMERIC)
                                                                                                                    END) + sum(CASE
                                                                                                                                   WHEN upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                        AND upper(pem.account_status_code) NOT IN('UB', 'BD')
                                                                                                                                        AND pem.payor_balance_amt_ins3 > 0
                                                                                                                                        AND pem.financial_class_code_ins3 = 99 THEN pem.payor_balance_amt_ins3
                                                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                                                               END) - sum(CASE
                                                                                                                                              WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA')
                                                                                                                                                   AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                   AND pem.total_account_balance_amt > 0
                                                                                                                                                   AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.total_account_balance_amt
                                                                                                                                              ELSE CAST(0 AS NUMERIC)
                                                                                                                                          END) AS non_sec_self_pay_ar_amt, sum(CASE
                                                                                                                                                                                   WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                        AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                        AND pem.total_account_balance_amt > 0
                                                                                                                                                                                        AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                             AND pem.financial_class_code_ins1 = 99.0) THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                               END) + sum(CASE
                                                                                                                                                                                              WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                   AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                   AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                   AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                        AND pem.financial_class_code_ins2 = 99.0) THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                              ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                          END) + sum(CASE
                                                                                                                                                                                                         WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                              AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                              AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                              AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                   AND pem.financial_class_code_ins3 = 99.0) THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                         ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                     END) AS totuninsureddiscount, sum(CASE
                                                                                                                                                                                                                                           WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                     AND pem.financial_class_code_ins1 = 15.0) THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                           ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                       END) + sum(CASE
                                                                                                                                                                                                                                                      WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                           AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                           AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                           AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                AND pem.financial_class_code_ins2 = 15.0) THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                      ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                  END) + sum(CASE
                                                                                                                                                                                                                                                                 WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                      AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                      AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                      AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                           AND pem.financial_class_code_ins3 = 15.0) THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                 ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                             END) AS totalcharityexpansiondiscount, sum(CASE
                                                                                                                                                                                                                                                                                                            WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                 AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                 AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                 AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                      AND pem.financial_class_code_ins1 = 15)
                                                                                                                                                                                                                                                                                                                 AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                            ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                        END) + sum(CASE
                                                                                                                                                                                                                                                                                                                       WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                            AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                            AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                            AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                 AND pem.financial_class_code_ins2 = 15)
                                                                                                                                                                                                                                                                                                                            AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                       ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                   END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                  WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                       AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                       AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                       AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                            AND pem.financial_class_code_ins3 = 15)
                                                                                                                                                                                                                                                                                                                                       AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                  ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                              END) AS secondaryagycharityexpdiscount, sum(CASE
                                                                                                                                                                                                                                                                                                                                                                              WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                   AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                   AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                   AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                        AND pem.financial_class_code_ins1 = 99)
                                                                                                                                                                                                                                                                                                                                                                                   AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                                                                                              ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                          END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                         WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                              AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                              AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                              AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                   AND pem.financial_class_code_ins2 = 99)
                                                                                                                                                                                                                                                                                                                                                                                              AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                                                                                         ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                     END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                    WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                         AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                         AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                         AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                              AND pem.financial_class_code_ins3 = 99)
                                                                                                                                                                                                                                                                                                                                                                                                         AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                                                                                    ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                END) AS secondaryagyuninsureddisc, sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                           WHEN pem.total_account_balance_amt > 0 THEN coalesce(ar_plp.ar_transaction_amt, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                                                                                                                                                                                                                                                                                           ELSE CAST(0 AS BIGNUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                       END) AS plp_amt,
                                                                                                                                                                                                                                                                                                                                                                                                                                   sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                           WHEN pem.total_account_balance_amt > 0 THEN coalesce(ar_plp_sec.ar_transaction_amt, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                                                                                                                                                                                                                                                                                           ELSE CAST(0 AS BIGNUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                       END) AS sec_plp_amt,
                                                                                                                                                                                                                                                                                                                                                                                                                                   any_value(pat_liability_protection_discount) AS pat_liability_protection_discount,
                                                                                                                                                                                                                                                                                                                                                                                                                                   sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                           WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND pem.financial_class_code_ins1 = 99.0) THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                                                                                                                                                           ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                       END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                      WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND pem.financial_class_code_ins2 = 99.0) THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                                                                                                                                                      ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                  END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                 WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                      AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND pem.financial_class_code_ins3 = 99.0) THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                                                                                                                                                 ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                             END) + (sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                             WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AND pem.financial_class_code_ins1 = 15.0) THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                             ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                         END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  AND pem.financial_class_code_ins2 = 15.0) THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AND pem.financial_class_code_ins3 = 15.0) THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               END)) - (sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          AND pem.financial_class_code_ins1 = 99)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AND pem.financial_class_code_ins2 = 99)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AND pem.financial_class_code_ins3 = 99)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  END)) - (sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND (CAST(pem.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AND pem.financial_class_code_ins1 = 15)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins1 + pem.payor_adjustment_amt_ins1
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AND (CAST(pem.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AND pem.financial_class_code_ins2 = 15)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins2 + pem.payor_adjustment_amt_ins2
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          END) + sum(CASE
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         WHEN upper(pem.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              AND upper(format_date('%Y%m', pem.discharge_date)) <= upper(pem.rptg_period)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              AND pem.total_account_balance_amt > 0
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              AND (CAST(pem.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AND pem.financial_class_code_ins3 = 15)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              AND upper(trim(pem.collector_org_type_code)) = 'S' THEN pem.payor_contract_allow_amt_ins3 + pem.payor_adjustment_amt_ins3
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     END)) + any_value(pat_liability_protection_discount) AS non_sec_uninsured_discount,
                                                                                                                                                                                                                                                                                                                                                                                                                                   sum(NUMERIC '0.000') AS transaction_amt
      FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom AS pem
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd ON upper(fd.unit_num) = upper(pem.unit_num)
      LEFT OUTER JOIN
        (SELECT max(k_0.coid) AS coid,
                k_0.pat_acct_num,
                sum(k_0.ar_transaction_amt) AS ar_transaction_amt
         FROM
           (SELECT max(a.coid) AS coid,
                   a.pat_acct_num,
                   sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE pass_eom.total_account_balance_amt > 0
                 AND upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) ) AS pa ON upper(pa.coid) = upper(a.coid)
            AND pa.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.transaction_code IN(784984,
                                        784985)
              AND a.iplan_id IN(9929,
                                0)
            GROUP BY upper(a.coid),
                     2
            UNION ALL SELECT max(a.coid) AS coid,
                             a.pat_acct_num,
                             sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE pass_eom.total_account_balance_amt > 0
                 AND upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) ) AS pa ON upper(pa.coid) = upper(a.coid)
            AND pa.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.transaction_code IN(999999)
              AND a.iplan_id = 9929
            GROUP BY upper(a.coid),
                     2
            UNION ALL SELECT max(a.coid) AS coid,
                             a.pat_acct_num,
                             sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE pass_eom.total_account_balance_amt > 0
                 AND upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) ) AS pa ON upper(pa.coid) = upper(a.coid)
            AND pa.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.iplan_id = 9929
              AND a.transaction_code IN(920972,
                                        920970,
                                        920980,
                                        920971,
                                        920981,
                                        920982)
            GROUP BY upper(a.coid),
                     2) AS k_0
         GROUP BY upper(k_0.coid),
                  2) AS ar_plp ON upper(ar_plp.coid) = upper(pem.coid)
      AND ar_plp.pat_acct_num = pem.pat_acct_num
      LEFT OUTER JOIN
        (SELECT max(k1.coid) AS coid,
                k1.pat_acct_num,
                sum(k1.ar_transaction_amt) AS ar_transaction_amt
         FROM
           (SELECT max(a.coid) AS coid,
                   a.pat_acct_num,
                   sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
                 AND upper(pass_eom.collector_org_type_code) = 'S'
                 AND pass_eom.total_account_balance_amt > 0 ) AS p ON upper(p.coid) = upper(a.coid)
            AND p.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.transaction_code IN(784984,
                                        784985)
              AND a.iplan_id IN(9929,
                                0)
            GROUP BY upper(a.coid),
                     2
            UNION ALL SELECT max(a.coid) AS coid,
                             a.pat_acct_num,
                             sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
                 AND upper(pass_eom.collector_org_type_code) = 'S'
                 AND pass_eom.total_account_balance_amt > 0 ) AS p ON upper(p.coid) = upper(a.coid)
            AND p.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.transaction_code IN(999999)
              AND a.iplan_id = 9929
            GROUP BY upper(a.coid),
                     2
            UNION ALL SELECT max(a.coid) AS coid,
                             a.pat_acct_num,
                             sum(a.ar_transaction_amt) AS ar_transaction_amt
            FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS a
            INNER JOIN
              (SELECT pass_eom.*
               FROM {{ params.param_auth_base_views_dataset_name }}.pass_eom
               WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
                 AND upper(pass_eom.collector_org_type_code) = 'S'
                 AND pass_eom.total_account_balance_amt > 0 ) AS p ON upper(p.coid) = upper(a.coid)
            AND p.pat_acct_num = a.pat_acct_num
            WHERE CASE a.transaction_type_code
                      WHEN '' THEN 0.0
                      ELSE CAST(a.transaction_type_code AS FLOAT64)
                  END IN(4,
                         5)
              AND a.iplan_id = 9929
              AND a.transaction_code IN(920972,
                                        920970,
                                        920980,
                                        920971,
                                        920981,
                                        920982)
            GROUP BY upper(a.coid),
                     2) AS k1
         GROUP BY upper(k1.coid),
                  2) AS ar_plp_sec ON upper(ar_plp_sec.coid) = upper(pem.coid)
      AND ar_plp_sec.pat_acct_num = pem.pat_acct_num
      CROSS JOIN UNNEST(ARRAY[ date_sub(CAST(trim(concat(format_date('%Y-%m', current_date('US/Central')), '-01')) AS DATE), interval 1 DAY) ]) AS eom_end
      CROSS JOIN UNNEST(ARRAY[ coalesce(sum(CASE
                                                WHEN pem.total_account_balance_amt > 0 THEN coalesce(ar_plp.ar_transaction_amt, CAST(0 AS BIGNUMERIC))
                                                ELSE CAST(0 AS BIGNUMERIC)
                                            END), CAST(0 AS BIGNUMERIC)) - coalesce(sum(CASE
                                                                                            WHEN pem.total_account_balance_amt > 0 THEN coalesce(ar_plp_sec.ar_transaction_amt, CAST(0 AS BIGNUMERIC))
                                                                                            ELSE CAST(0 AS BIGNUMERIC)
                                                                                        END), CAST(0 AS BIGNUMERIC)) ]) AS pat_liability_protection_discount
      WHERE upper(pem.rptg_period) = upper(format_date('%Y%m', eom_end))
        AND left(pem.patient_type_code, 1) IN('O',
                                              'E',
                                              'S',
                                              'I')
        AND upper(fd.summary_7_member_ind) = 'Y'
        AND upper(fd.company_code) = 'H'
      GROUP BY 1) AS in_query
   GROUP BY 1
   UNION DISTINCT SELECT eom_end AS eom_end,
                         CAST(sum(0) AS BIGNUMERIC) AS non_sec_self_pay_ar_amt,
                         CAST(sum(0) AS BIGNUMERIC) AS non_sec_uninsured_discount,
                         sum(CASE
                                 WHEN upper(ar.ar_transaction_comment_text) = 'BAD DEBT'
                                      AND upper(ar.transaction_type_code) = '3'
                                      AND (upper(fp.account_status_code) <> 'IN'
                                           OR fp.account_status_code IS NULL) THEN ar.ar_transaction_amt
                                 WHEN upper(ar.transaction_type_code) IN('4', '5')
                                      AND upper(ar.debit_gl_dept_num) = '110'
                                      AND upper(ar.debit_gl_sub_account_num) = '090' THEN ar.ar_transaction_amt
                                 ELSE CAST(0 AS NUMERIC)
                             END) AS transaction_amt
   FROM {{ params.param_auth_base_views_dataset_name }}.ar_transaction_mv AS ar
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_patient AS fp ON ar.patient_dw_id = fp.patient_dw_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd ON upper(fd.unit_num) = upper(fp.unit_num)
   CROSS JOIN UNNEST(ARRAY[ date_sub(CAST(trim(concat(format_date('%Y-%m', current_date('US/Central')), '-01')) AS DATE), interval 1 DAY) ]) AS eom_end
   WHERE (ar.ar_transaction_effective_date BETWEEN CAST(trim(concat(format_date('%Y-%m', eom_end), '-01')) AS DATE) AND eom_end
          AND ar.ar_transaction_enter_date BETWEEN CAST(trim(concat(format_date('%Y-%m', eom_end), '-01')) AS DATE) AND date_add(eom_end, interval 4 DAY)
          OR ar.ar_transaction_enter_date BETWEEN CAST(trim(concat(format_date('%Y-%m', eom_end), '-05')) AS DATE) AND date_add(eom_end, interval 4 DAY)
          AND ar.ar_transaction_effective_date < date_add(eom_end, interval 1 DAY))
     AND upper(ar.transaction_type_code) IN('3',
                                            '4',
                                            '5')
     AND (upper(fp.patient_type_code_pos1) IN('O',
                                              'E',
                                              'S',
                                              'I',
                                              'N')
          OR fp.patient_type_code_pos1 IS NULL)
     AND upper(fd.summary_7_member_ind) = 'Y'
     AND upper(fd.company_code) = 'H'
   GROUP BY 1) AS k ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(coalesce(trim(CAST(sum(src.sum1) AS STRING)), '0'), coalesce(trim(CAST(sum(src.sum2) AS STRING)), '0'), coalesce(trim(CAST(sum(src.sum3) AS STRING)), '0')) AS source_string
FROM
  (SELECT sum(x.bad_debt_writeoff_amt) AS sum1,
          sum(x.non_secn_self_pay_ar_amt) AS sum2,
          sum(x.non_secn_unins_disc_amt) AS sum3
   FROM {{ params.param_pbs_core_dataset_name }}.rpt_ar_ada_detail AS x
   WHERE x.month_id = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                          WHEN '' THEN 0.0
                          ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS FLOAT64)
                      END ) AS src ;)
);

SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

LOOP
  SET idx = idx + 1;

  IF idx > idx_length THEN
    BREAK;
  END IF;

  SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
  SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

  SET difference = 
    CASE 
    WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
    WHEN expected_value = 0 and actual_value = 0 Then 0
    ELSE actual_value
    END;

  SET audit_status = 
  CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
  END;

  IF idx = 1 THEN
    SET audit_type = "RECORD_COUNT";
  ELSE
    SET audit_type = CONCAT("VALIDATION_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO "{{ params.param_pbs_audit_dataset_name }}".audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, job_name, audit_time, audit_status
   );

END LOOP;
