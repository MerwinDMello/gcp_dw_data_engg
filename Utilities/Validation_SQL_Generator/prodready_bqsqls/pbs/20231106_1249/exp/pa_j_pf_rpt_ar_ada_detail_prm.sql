-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pa_j_pf_rpt_ar_ada_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(trim(regexp_replace(format('%#17.2f', CAST(ROUND(sum(k.transaction_amt), 2, 'ROUND_HALF_EVEN') AS NUMERIC)), r'^(.*?)(-)?0(\..*)', r'\1 \2\3')), trim(CAST(sum(k.non_sec_self_pay_ar_amt) AS STRING)), trim(CAST(sum(k.non_sec_uninsured_discount) AS STRING))) AS source_string
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
                                                                                                                                          END) AS non_sec_self_pay_ar_amt, -- AND PEM.Collector_Org_Code IN (858,859,860,865,867,703)
 /*TotUninsuredDiscount*/ -- -Added 9943 and 9948
 -- ---Added 9943 and 9948
 -- and financial_class_code='99'
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
                           END) AS totuninsureddiscount, -- ---Added 9943 and 9948
 -- and financial_class_code='99'
 /*TotalCharityExpansionDiscount*/ sum(CASE
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
                                                             END) AS totalcharityexpansiondiscount, /*SecondaryAgencyCharityExpansionDiscount*/ sum(CASE
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
                                                                                                                                                                          END) AS secondaryagycharityexpdiscount, /*SecondaryAgyUninsuredDisc*/ -- Added 9943 and 9948
 -- Added 9943 and 9948
 sum(CASE
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
                           END) AS secondaryagyuninsureddisc, -- Added 9943 and 9948
 sum(CASE
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
   GROUP BY 1) AS k