-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/pa_j_ada_patient_level_detail_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(coalesce(trim(CAST(sum(src.ihch) AS STRING)), '0'), coalesce(trim(CAST(sum(src.insfc15) AS STRING)), '0'), coalesce(trim(CAST(sum(src.insfc99) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totuninsureddiscount) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalcharityexpansiondiscount) AS STRING)), '0'), coalesce(trim(CAST(sum(src.plp) AS STRING)), '0'), coalesce(trim(CAST(sum(src.total_discounts) AS STRING)), '0'), coalesce(trim(CAST(sum(src.secondaryagyuninsureddisc) AS STRING)), '0'), coalesce(trim(CAST(sum(src.secondaryagencycharityexpansiondiscount_ins) AS STRING)), '0'), coalesce(trim(CAST(sum(src.sec_plp) AS STRING)), '0'), coalesce(trim(CAST(sum(src.secondary_agency_discounts) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalnonsecondaryuninsureddiscount) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalnonsecondarycharityexpansiondiscount) AS STRING)), '0'), coalesce(trim(CAST(sum(src.total_nonsecondary_pat_liability_protection_discount) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalnonsecondarydiscounts) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalselfpayar) AS STRING)), '0'), coalesce(trim(CAST(sum(src.secondarytotacctbal) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalnonsecondaryselfpayar) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totalgrossnonsecondaryselfpayar) AS STRING)), '0'), coalesce(trim(CAST(sum(src.totpatientdue) AS STRING)), '0'), coalesce(trim(CAST(sum(src.dnfbsp) AS STRING)), '0'), coalesce(trim(CAST(sum(src.ihsp) AS STRING)), '0'), coalesce(trim(CAST(sum(src.dnfbch) AS STRING)), '0')) AS source_string
FROM
  (SELECT sum(CASE
                  WHEN peom.financial_class_code = 15
                       AND (upper(format_date('%Y%m', peom.discharge_date)) > upper(peom.rptg_period)
                            OR upper(peom.account_status_code) = 'UB'
                            AND peom.discharge_date IS NOT NULL
                            AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                            AND peom.total_account_balance_amt <> 0) THEN peom.total_account_balance_amt
                  ELSE CAST(0 AS NUMERIC)
              END) AS dnfbch, -- Discharged_Not_Final_Bill_Charity_Amt
 sum(CASE
         WHEN (upper(peom.account_status_code) = 'UB'
               AND upper(substr(peom.patient_type_code, 2, 1)) <> 'P'
               AND peom.discharge_date IS NULL
               AND peom.total_account_balance_amt <> 0
               OR upper(peom.account_status_code) = 'UB'
               AND upper(peom.unbill_pre_admit_ind) = 'Y')
              AND peom.financial_class_code = 15 THEN peom.total_account_balance_amt
         ELSE CAST(0 AS NUMERIC)
     END) AS ihch, -- Inhouse_Charity_Amt
 sum(CASE
         WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND upper(peom.account_status_code) NOT IN('UB', 'BD')
              AND peom.payor_balance_amt_ins1 > 0
              AND peom.financial_class_code_ins1 = 15 THEN peom.payor_balance_amt_ins1
         ELSE CAST(0 AS NUMERIC)
     END) + sum(CASE
                    WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                         AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                         AND peom.payor_balance_amt_ins2 > 0
                         AND peom.financial_class_code_ins2 = 15 THEN peom.payor_balance_amt_ins2
                    ELSE CAST(0 AS NUMERIC)
                END) + sum(CASE
                               WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                    AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                    AND peom.payor_balance_amt_ins3 > 0
                                    AND peom.financial_class_code_ins3 = 15 THEN peom.payor_balance_amt_ins3
                               ELSE CAST(0 AS NUMERIC)
                           END) AS insfc15, -- Insured_Charity_Amt
 sum(CASE
         WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND upper(peom.account_status_code) NOT IN('UB', 'BD')
              AND peom.payor_balance_amt_ins1 > 0
              AND peom.financial_class_code_ins1 = 99 THEN peom.payor_balance_amt_ins1
         ELSE CAST(0 AS NUMERIC)
     END) + sum(CASE
                    WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                         AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                         AND peom.payor_balance_amt_ins2 > 0
                         AND peom.financial_class_code_ins2 = 99 THEN peom.payor_balance_amt_ins2
                    ELSE CAST(0 AS NUMERIC)
                END) + sum(CASE
                               WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                    AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                    AND peom.payor_balance_amt_ins3 > 0
                                    AND peom.financial_class_code_ins3 = 99 THEN peom.payor_balance_amt_ins3
                               ELSE CAST(0 AS NUMERIC)
                           END) AS insfc99, -- Insured_Self_Pay_Amt
 -- -ADDED 9943 AND 9948
 -- ---ADDED 9943 AND 9948
 -- AND FINANCIAL_CLASS_CODE='99'
 sum(CASE
         WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
              AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND peom.total_account_balance_amt > 0
              AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                   AND peom.financial_class_code_ins1 = 99.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
         ELSE CAST(0 AS NUMERIC)
     END) + sum(CASE
                    WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                         AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                         AND peom.total_account_balance_amt > 0
                         AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                              AND peom.financial_class_code_ins2 = 99.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                    ELSE CAST(0 AS NUMERIC)
                END) + sum(CASE
                               WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                    AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                    AND peom.total_account_balance_amt > 0
                                    AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                         AND peom.financial_class_code_ins3 = 99.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                               ELSE CAST(0 AS NUMERIC)
                           END) AS totuninsureddiscount, -- ---ADDED 9943 AND 9948
 -- AND FINANCIAL_CLASS_CODE='99'
 -- Uninsured_Discount_Amt
 sum(CASE
         WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
              AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND peom.total_account_balance_amt > 0
              AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                   AND peom.financial_class_code_ins1 = 15.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
         ELSE CAST(0 AS NUMERIC)
     END) + sum(CASE
                    WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                         AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                         AND peom.total_account_balance_amt > 0
                         AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                              AND peom.financial_class_code_ins2 = 15.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                    ELSE CAST(0 AS NUMERIC)
                END) + sum(CASE
                               WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                    AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                    AND peom.total_account_balance_amt > 0
                                    AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                         AND peom.financial_class_code_ins3 = 15.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                               ELSE CAST(0 AS NUMERIC)
                           END) AS totalcharityexpansiondiscount, -- Charity_Discount_Amt
 sum(ar_plp.ar_transaction_amt) AS plp, -- Patient_Liab_Prot_Discount_Amt
 sum(CASE
         WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
              AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND peom.total_account_balance_amt > 0
              AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                   AND peom.financial_class_code_ins1 = 99.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
         ELSE CAST(0 AS NUMERIC)
     END) + sum(CASE
                    WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                         AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                         AND peom.total_account_balance_amt > 0
                         AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                              AND peom.financial_class_code_ins2 = 99.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                    ELSE CAST(0 AS NUMERIC)
                END) + sum(CASE
                               WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                    AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                    AND peom.total_account_balance_amt > 0
                                    AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                         AND peom.financial_class_code_ins3 = 99.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                               ELSE CAST(0 AS NUMERIC)
                           END) + (sum(CASE
                                           WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                AND peom.total_account_balance_amt > 0
                                                AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                     AND peom.financial_class_code_ins1 = 15.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                           ELSE CAST(0 AS NUMERIC)
                                       END) + sum(CASE
                                                      WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                           AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                           AND peom.total_account_balance_amt > 0
                                                           AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                AND peom.financial_class_code_ins2 = 15.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                      ELSE CAST(0 AS NUMERIC)
                                                  END) + sum(CASE
                                                                 WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                      AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                      AND peom.total_account_balance_amt > 0
                                                                      AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                           AND peom.financial_class_code_ins3 = 15.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                 ELSE CAST(0 AS NUMERIC)
                                                             END)) + sum(ar_plp.ar_transaction_amt) AS total_discounts, -- Total_Discount_Amt
 coalesce(sum(CASE
                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                       AND peom.total_account_balance_amt > 0
                       AND CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                       AND upper(peom.collector_org_type_code) = 'S'
                       AND peom.financial_class_code_ins1 = 99.0 THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                  ELSE CAST(0 AS NUMERIC)
              END) + sum(CASE
                             WHEN upper(peom.account_status_code) IN(-- AND PEOM.COLLECTOR_ORG_CODE IN (858,859,860)
 'AR', 'AX', 'CA', 'UB')
                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                  AND peom.total_account_balance_amt > 0
                                  AND CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                  AND upper(peom.collector_org_type_code) = 'S'
                                  AND peom.financial_class_code_ins2 = 99.0 THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                             ELSE CAST(0 AS NUMERIC)
                         END) + sum(CASE
                                        WHEN upper(peom.account_status_code) IN(-- AND PEOM.COLLECTOR_ORG_CODE IN (858,859,860)
 'AR', 'AX', 'CA', 'UB')
                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                             AND peom.total_account_balance_amt > 0
                                             AND CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                             AND upper(peom.collector_org_type_code) = 'S'
                                             AND peom.financial_class_code_ins3 = 99.0 THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                        ELSE CAST(0 AS NUMERIC)
                                    END), NUMERIC '0.00') AS secondaryagyuninsureddisc, -- AND PEOM.COLLECTOR_ORG_CODE IN (858,859,860)
 -- Secn_Agcy_Unins_Discount_Amt
 coalesce(sum(CASE
                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                       AND peom.total_account_balance_amt > 0
                       AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                            AND peom.financial_class_code_ins1 = 15.0)
                       AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                  ELSE CAST(0 AS NUMERIC)
              END) + sum(CASE
                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                  AND peom.total_account_balance_amt > 0
                                  AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                       AND peom.financial_class_code_ins2 = 15.0)
                                  AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                             ELSE CAST(0 AS NUMERIC)
                         END) + sum(CASE
                                        WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                             AND peom.total_account_balance_amt > 0
                                             AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                  AND peom.financial_class_code_ins3 = 15.0)
                                             AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                        ELSE CAST(0 AS NUMERIC)
                                    END), CAST(0 AS BIGNUMERIC)) AS secondaryagencycharityexpansiondiscount_ins, -- Secn_Agcy_Charity_Discount_Amt
 sum(ar_plp_sec.ar_transaction_amt) AS sec_plp, -- Secn_Agcy_Pat_Liab_Prot_Discount_Amt
 coalesce(sum(CASE
                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                       AND peom.total_account_balance_amt > 0
                       AND CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                       AND upper(peom.collector_org_type_code) = 'S'
                       AND peom.financial_class_code_ins1 = 99.0 THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                  ELSE CAST(0 AS NUMERIC)
              END) + sum(CASE
                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                  AND peom.total_account_balance_amt > 0
                                  AND CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                  AND upper(peom.collector_org_type_code) = 'S'
                                  AND peom.financial_class_code_ins2 = 99.0 THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                             ELSE CAST(0 AS NUMERIC)
                         END) + sum(CASE
                                        WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                             AND peom.total_account_balance_amt > 0
                                             AND CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                             AND upper(peom.collector_org_type_code) = 'S'
                                             AND peom.financial_class_code_ins3 = 99.0 THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                        ELSE CAST(0 AS NUMERIC)
                                    END), NUMERIC '0.00') + coalesce(sum(CASE
                                                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                  AND peom.total_account_balance_amt > 0
                                                                                  AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                       AND peom.financial_class_code_ins1 = 15.0)
                                                                                  AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                                                             ELSE CAST(0 AS NUMERIC)
                                                                         END) + sum(CASE
                                                                                        WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                             AND peom.total_account_balance_amt > 0
                                                                                             AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                  AND peom.financial_class_code_ins2 = 15.0)
                                                                                             AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                                                        ELSE CAST(0 AS NUMERIC)
                                                                                    END) + sum(CASE
                                                                                                   WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                        AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                        AND peom.total_account_balance_amt > 0
                                                                                                        AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                             AND peom.financial_class_code_ins3 = 15.0)
                                                                                                        AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                               END), CAST(0 AS BIGNUMERIC)) + sum(ar_plp_sec.ar_transaction_amt) AS secondary_agency_discounts, -- Total_Secn_Agcy_Discount_Amt
 any_value(totalnonsecondaryuninsureddiscount) AS totalnonsecondaryuninsureddiscount, -- Non_Secn_Unins_Disc_Amt
 any_value(totalnonsecondarycharityexpansiondiscount) AS totalnonsecondarycharityexpansiondiscount, -- Non_Secn_Charity_Discount_Amt
 any_value(total_nonsecondary_pat_liability_protection_discount) AS total_nonsecondary_pat_liability_protection_discount, -- Non_Secn_Patient_Liab_Prot_Discount_Amt
 any_value(totalnonsecondarydiscounts) AS totalnonsecondarydiscounts, --  Total_Non_Secn_Discount_Amt --ADA SPCA
 coalesce(sum(CASE
                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA')
                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                       AND peom.total_account_balance_amt > 0
                       AND upper(peom.collector_org_type_code) = 'S' THEN peom.total_account_balance_amt
                  ELSE CAST(0 AS NUMERIC)
              END), NUMERIC '0.00') AS secondarytotacctbal, -- AND PEOM.COLLECTOR_ORG_CODE IN (858,859,860)
 --  Secn_Agcy_Acct_Bal_Amt
 sum(CASE
         WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA')
              AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
              AND peom.total_account_balance_amt > 0 THEN peom.patient_balance_amt
         ELSE CAST(0 AS NUMERIC)
     END) AS totpatientdue, -- Total_Patient_Due_Amt
 sum(CASE
         WHEN (upper(format_date('%Y%m', peom.discharge_date)) > upper(peom.rptg_period)
               OR upper(peom.account_status_code) = 'UB'
               AND peom.discharge_date IS NOT NULL
               AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
               AND peom.total_account_balance_amt <> 0)
              AND peom.financial_class_code = 99 THEN peom.total_account_balance_amt
         ELSE CAST(0 AS NUMERIC)
     END) AS dnfbsp, -- Discharged_Not_Final_Bill_Self_Pay_Amt
 sum(CASE
         WHEN peom.financial_class_code = 99
              AND (upper(peom.account_status_code) = 'UB'
                   AND upper(substr(peom.patient_type_code, 2, 1)) <> 'P'
                   AND peom.discharge_date IS NULL
                   AND peom.total_account_balance_amt <> 0
                   OR upper(peom.account_status_code) = 'UB'
                   AND upper(peom.unbill_pre_admit_ind) = 'Y') THEN peom.total_account_balance_amt
         ELSE CAST(0 AS NUMERIC)
     END) AS ihsp, -- Inhouse_Self_Pay_Amt
 any_value(totalselfpayar) AS totalselfpayar, --  Self_Pay_AR_Amt
 any_value(totalnonsecondaryselfpayar) AS totalnonsecondaryselfpayar, --  Non_Secn_Self_Pay_AR_Amt
 any_value(totalnonsecondaryselfpayar) + any_value(totalnonsecondarydiscounts) AS totalgrossnonsecondaryselfpayar
   FROM --  Gross_Non_Secn_Self_Pay_AR_Amt
 `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom AS peom
   INNER JOIN
     (SELECT fd.*
      FROM `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension_eom AS fd
      WHERE upper(fd.summary_7_member_ind) = 'Y'
        AND upper(fd.company_code) = 'H'
      UNION ALL SELECT facility_dimension_eom.*
      FROM `hca-hin-dev-cur-parallon`.edwpbs_base_views.facility_dimension_eom
      WHERE upper(facility_dimension_eom.coid) IN
          (SELECT DISTINCT upper(dim_esb_organization_homehealth_dnd.coid) AS coid
           FROM `hca-hin-dev-cur-parallon`.edwpbs_staging.dim_esb_organization_homehealth_dnd
           WHERE dim_esb_organization_homehealth_dnd.coid IS NOT NULL ) ) AS f ON upper(f.unit_num) = upper(peom.unit_num)
   LEFT OUTER JOIN
     (SELECT max(k.coid) AS coid,
             k.pat_acct_num,
             sum(k.ar_transaction_amt) AS ar_transaction_amt
      FROM
        (SELECT max(a.coid) AS coid,
                a.pat_acct_num,
                sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0 ) AS pa ON upper(pa.coid) = upper(a.coid)
         AND pa.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5) AND
 784984.0,
 784985.0)
           AND a.iplan_id IN(9929,
                             0)
         GROUP BY upper(a.coid),
                  2
         UNION ALL SELECT --  AND A.COID=34632
 max(a.coid) AS coid,
 a.pat_acct_num,
 sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0 ) AS pa ON upper(pa.coid) = upper(a.coid)
         AND pa.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5) AND
 999999.0)
           AND a.iplan_id = 9929
         GROUP BY upper(a.coid),
                  2
         UNION ALL SELECT -- AND A.COID=34632
 max(a.coid) AS coid,
 a.pat_acct_num,
 sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0 ) AS pa ON upper(pa.coid) = upper(a.coid)
         AND pa.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND a.iplan_id = 9929
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AND AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5)
 920972.0,
 920970.0,
 920980.0,
 920971.0,
 920981.0,
 920982.0)
         GROUP BY upper(a.coid),
                  2) AS k
      GROUP BY upper(k.coid),
               2) AS ar_plp ON upper(ar_plp.coid) = upper(peom.coid)
   AND ar_plp.pat_acct_num = peom.pat_acct_num
   LEFT OUTER JOIN --  AND A.COID=34632

     (SELECT max(k1.coid) AS coid,
             k1.pat_acct_num,
             sum(k1.ar_transaction_amt) AS ar_transaction_amt
      FROM
        (SELECT max(a.coid) AS coid,
                a.pat_acct_num,
                sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0
              AND upper(pass_eom.collector_org_type_code) = 'S' ) AS pe ON upper(pe.coid) = upper(a.coid)
         AND pe.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5) AND
 784984.0,
 784985.0)
           AND a.iplan_id IN(9929,
                             0)
         GROUP BY upper(a.coid),
                  2
         UNION ALL SELECT max(a.coid) AS coid,
                          a.pat_acct_num,
                          sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0
              AND upper(pass_eom.collector_org_type_code) = 'S' ) AS pe ON upper(pe.coid) = upper(a.coid)
         AND pe.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5) AND
 999999.0)
           AND a.iplan_id = 9929
         GROUP BY upper(a.coid),
                  2
         UNION ALL SELECT max(a.coid) AS coid,
                          a.pat_acct_num,
                          sum(a.ar_transaction_amt) AS ar_transaction_amt
         FROM -- FROM EDWPF_VIEWS.AR_TRANSACTION A
 `hca-hin-dev-cur-parallon`.auth_base_views.ar_transaction_mv AS a
         INNER JOIN
           (SELECT pass_eom.*
            FROM `hca-hin-dev-cur-parallon`.auth_base_views.pass_eom
            WHERE upper(pass_eom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
              AND pass_eom.total_account_balance_amt > 0
              AND upper(pass_eom.collector_org_type_code) = 'S' ) AS pe ON upper(pe.coid) = upper(a.coid)
         AND pe.pat_acct_num = a.pat_acct_num
         WHERE upper(a.transaction_type_code) IN('4',
                                                 '5')
           AND a.iplan_id = 9929
           AND CAST(a.transaction_code AS FLOAT64) IN(-- AND AR_TRANSACTION_ENTER_DATE < ( current_date - EXTRACT(DAY FROM ADD_MONTHS(CURRENT_DATE,-1)) + 5)
 920972.0,
 920970.0,
 920980.0,
 920971.0,
 920981.0,
 920982.0)
         GROUP BY upper(a.coid),
                  2) AS k1
      GROUP BY upper(k1.coid),
               2) AS ar_plp_sec ON upper(ar_plp_sec.coid) = upper(peom.coid)
   AND ar_plp_sec.pat_acct_num = peom.pat_acct_num
   CROSS JOIN UNNEST(ARRAY[ coalesce(sum(CASE
                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA')
                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                  AND peom.total_account_balance_amt > 0 THEN peom.patient_balance_amt
                                             ELSE CAST(0 AS NUMERIC)
                                         END) + sum(CASE
                                                        WHEN (upper(format_date('%Y%m', peom.discharge_date)) > upper(peom.rptg_period)
                                                              OR upper(peom.account_status_code) = 'UB'
                                                              AND peom.discharge_date IS NOT NULL
                                                              AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                              AND peom.total_account_balance_amt <> 0)
                                                             AND peom.financial_class_code = 99 THEN peom.total_account_balance_amt
                                                        ELSE CAST(0 AS NUMERIC)
                                                    END) + sum(CASE
                                                                   WHEN peom.financial_class_code = 99
                                                                        AND (upper(peom.account_status_code) = 'UB'
                                                                             AND upper(substr(peom.patient_type_code, 2, 1)) <> 'P'
                                                                             AND peom.discharge_date IS NULL
                                                                             AND peom.total_account_balance_amt <> 0
                                                                             OR upper(peom.account_status_code) = 'UB'
                                                                             AND upper(peom.unbill_pre_admit_ind) = 'Y') THEN peom.total_account_balance_amt
                                                                   ELSE CAST(0 AS NUMERIC)
                                                               END) + sum(CASE
                                                                              WHEN peom.financial_class_code = 15
                                                                                   AND (upper(format_date('%Y%m', peom.discharge_date)) > upper(peom.rptg_period)
                                                                                        OR upper(peom.account_status_code) = 'UB'
                                                                                        AND peom.discharge_date IS NOT NULL
                                                                                        AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                        AND peom.total_account_balance_amt <> 0) THEN peom.total_account_balance_amt
                                                                              ELSE CAST(0 AS NUMERIC)
                                                                          END) + sum(CASE
                                                                                         WHEN (upper(peom.account_status_code) = 'UB'
                                                                                               AND upper(substr(peom.patient_type_code, 2, 1)) <> 'P'
                                                                                               AND peom.discharge_date IS NULL
                                                                                               AND peom.total_account_balance_amt <> 0
                                                                                               OR upper(peom.account_status_code) = 'UB'
                                                                                               AND upper(peom.unbill_pre_admit_ind) = 'Y')
                                                                                              AND peom.financial_class_code = 15 THEN peom.total_account_balance_amt
                                                                                         ELSE CAST(0 AS NUMERIC)
                                                                                     END) + (sum(CASE
                                                                                                     WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                          AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                          AND peom.payor_balance_amt_ins1 > 0
                                                                                                          AND peom.financial_class_code_ins1 = 15 THEN peom.payor_balance_amt_ins1
                                                                                                     ELSE CAST(0 AS NUMERIC)
                                                                                                 END) + sum(CASE
                                                                                                                WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                     AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                                     AND peom.payor_balance_amt_ins2 > 0
                                                                                                                     AND peom.financial_class_code_ins2 = 15 THEN peom.payor_balance_amt_ins2
                                                                                                                ELSE CAST(0 AS NUMERIC)
                                                                                                            END) + sum(CASE
                                                                                                                           WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                                AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                                                AND peom.payor_balance_amt_ins3 > 0
                                                                                                                                AND peom.financial_class_code_ins3 = 15 THEN peom.payor_balance_amt_ins3
                                                                                                                           ELSE CAST(0 AS NUMERIC)
                                                                                                                       END)) + (sum(CASE
                                                                                                                                        WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                                             AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                                                             AND peom.payor_balance_amt_ins1 > 0
                                                                                                                                             AND peom.financial_class_code_ins1 = 99 THEN peom.payor_balance_amt_ins1
                                                                                                                                        ELSE CAST(0 AS NUMERIC)
                                                                                                                                    END) + sum(CASE
                                                                                                                                                   WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                                                        AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                                                                        AND peom.payor_balance_amt_ins2 > 0
                                                                                                                                                        AND peom.financial_class_code_ins2 = 99 THEN peom.payor_balance_amt_ins2
                                                                                                                                                   ELSE CAST(0 AS NUMERIC)
                                                                                                                                               END) + sum(CASE
                                                                                                                                                              WHEN upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                                                                   AND upper(peom.account_status_code) NOT IN('UB', 'BD')
                                                                                                                                                                   AND peom.payor_balance_amt_ins3 > 0
                                                                                                                                                                   AND peom.financial_class_code_ins3 = 99 THEN peom.payor_balance_amt_ins3
                                                                                                                                                              ELSE CAST(0 AS NUMERIC)
                                                                                                                                                          END)), NUMERIC '0.00') ]) AS totalselfpayar
   CROSS JOIN UNNEST(ARRAY[ coalesce(totalselfpayar - coalesce(sum(CASE
                                                                       WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA')
                                                                            AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                            AND peom.total_account_balance_amt > 0
                                                                            AND upper(peom.collector_org_type_code) = 'S' THEN peom.total_account_balance_amt
                                                                       ELSE CAST(0 AS NUMERIC)
                                                                   END), NUMERIC '0.00'), NUMERIC '0.00') ]) AS totalnonsecondaryselfpayar
   CROSS JOIN UNNEST(ARRAY[ coalesce(sum(ar_plp.ar_transaction_amt) - sum(ar_plp_sec.ar_transaction_amt), NUMERIC '0.00') ]) AS total_nonsecondary_pat_liability_protection_discount
   CROSS JOIN UNNEST(ARRAY[ coalesce(sum(CASE
                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                  AND peom.total_account_balance_amt > 0
                                                  AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                       AND peom.financial_class_code_ins1 = 15.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                             ELSE CAST(0 AS NUMERIC)
                                         END) + sum(CASE
                                                        WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                             AND peom.total_account_balance_amt > 0
                                                             AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                  AND peom.financial_class_code_ins2 = 15.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                        ELSE CAST(0 AS NUMERIC)
                                                    END) + sum(CASE
                                                                   WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                        AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                        AND peom.total_account_balance_amt > 0
                                                                        AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                             AND peom.financial_class_code_ins3 = 15.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                   ELSE CAST(0 AS NUMERIC)
                                                               END) - coalesce(sum(CASE
                                                                                       WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                            AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                            AND peom.total_account_balance_amt > 0
                                                                                            AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9927.0)
                                                                                                 AND peom.financial_class_code_ins1 = 15.0)
                                                                                            AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                                                                       ELSE CAST(0 AS NUMERIC)
                                                                                   END) + sum(CASE
                                                                                                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                       AND peom.total_account_balance_amt > 0
                                                                                                       AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9927.0)
                                                                                                            AND peom.financial_class_code_ins2 = 15.0)
                                                                                                       AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                                                                  ELSE CAST(0 AS NUMERIC)
                                                                                              END) + sum(CASE
                                                                                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                  AND peom.total_account_balance_amt > 0
                                                                                                                  AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9927.0)
                                                                                                                       AND peom.financial_class_code_ins3 = 15.0)
                                                                                                                  AND upper(peom.collector_org_type_code) = 'S' THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                                                             ELSE CAST(0 AS NUMERIC)
                                                                                                         END), CAST(0 AS BIGNUMERIC)), NUMERIC '0.00') ]) AS totalnonsecondarycharityexpansiondiscount
   CROSS JOIN UNNEST(ARRAY[ coalesce(sum(CASE
                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                  AND peom.total_account_balance_amt > 0
                                                  AND (CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                       AND peom.financial_class_code_ins1 = 99.0) THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                             ELSE CAST(0 AS NUMERIC)
                                         END) + sum(CASE
                                                        WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                             AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                             AND peom.total_account_balance_amt > 0
                                                             AND (CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                  AND peom.financial_class_code_ins2 = 99.0) THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                        ELSE CAST(0 AS NUMERIC)
                                                    END) + sum(CASE
                                                                   WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                        AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                        AND peom.total_account_balance_amt > 0
                                                                        AND (CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                             AND peom.financial_class_code_ins3 = 99.0) THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                   ELSE CAST(0 AS NUMERIC)
                                                               END) - coalesce(sum(CASE
                                                                                       WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                            AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                            AND peom.total_account_balance_amt > 0
                                                                                            AND CAST(peom.iplan_id_ins1 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                            AND upper(peom.collector_org_type_code) = 'S'
                                                                                            AND peom.financial_class_code_ins1 = 99.0 THEN peom.payor_contract_allow_amt_ins1 + peom.payor_adjustment_amt_ins1
                                                                                       ELSE CAST(0 AS NUMERIC)
                                                                                   END) + sum(CASE
                                                                                                  WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                       AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                       AND peom.total_account_balance_amt > 0
                                                                                                       AND CAST(peom.iplan_id_ins2 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                       AND upper(peom.collector_org_type_code) = 'S'
                                                                                                       AND peom.financial_class_code_ins2 = 99.0 THEN peom.payor_contract_allow_amt_ins2 + peom.payor_adjustment_amt_ins2
                                                                                                  ELSE CAST(0 AS NUMERIC)
                                                                                              END) + sum(CASE
                                                                                                             WHEN upper(peom.account_status_code) IN('AR', 'AX', 'CA', 'UB')
                                                                                                                  AND upper(format_date('%Y%m', peom.discharge_date)) <= upper(peom.rptg_period)
                                                                                                                  AND peom.total_account_balance_amt > 0
                                                                                                                  AND CAST(peom.iplan_id_ins3 AS FLOAT64) IN(9940.0, 9941.0, 9942.0, 9943.0, 9944.0, 9945.0, 9946.0, 9947.0, 9948.0, 9949.0)
                                                                                                                  AND upper(peom.collector_org_type_code) = 'S'
                                                                                                                  AND peom.financial_class_code_ins3 = 99.0 THEN peom.payor_contract_allow_amt_ins3 + peom.payor_adjustment_amt_ins3
                                                                                                             ELSE CAST(0 AS NUMERIC)
                                                                                                         END), NUMERIC '0.00'), NUMERIC '0.00') ]) AS totalnonsecondaryuninsureddiscount
   CROSS JOIN UNNEST(ARRAY[ totalnonsecondaryuninsureddiscount + totalnonsecondarycharityexpansiondiscount + total_nonsecondary_pat_liability_protection_discount ]) AS totalnonsecondarydiscounts
   WHERE upper(peom.rptg_period) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
     AND left(peom.patient_type_code, 1) IN('O',
                                            'E',
                                            'S',
                                            'I') ) AS src ;

-- AND PEOM.COID IN (34222)
-- AND F.SUMMARY_7_MEMBER_IND = 'Y' AND F.COMPANY_CODE = 'H'