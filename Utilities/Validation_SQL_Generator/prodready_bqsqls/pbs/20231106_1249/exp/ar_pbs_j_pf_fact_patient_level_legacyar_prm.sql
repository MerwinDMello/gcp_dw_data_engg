-- Translation time: 2023-11-06T19:10:57.497752Z
-- Translation job ID: 0a348d04-5c95-4dfe-94e8-9aaeb80a31a8
-- Source: eim-parallon-cs-datamig-dev-0002/pbs_bulk_conversion_validation/20231106_1249/input/exp/ar_pbs_j_pf_fact_patient_level_legacyar_prm.sql
-- Translated from: Teradata
-- Translated to: BigQuery
 -- locking table edwpbs_staging.pass_current for access

SELECT concat('PBMAR100', ',', trim(CAST(sum(coalesce(subq.eom_pymt_rcvd, NUMERIC '0')) AS STRING)), ',', trim(CAST(sum(CASE
                                                                                                                            WHEN subq.ins_order_num = 0
                                                                                                                                 AND upper(trim(subq.acct_stat)) <> 'UB' THEN coalesce(subq.eom_pat_bal, NUMERIC '0')
                                                                                                                            WHEN subq.ins_order_num = 0
                                                                                                                                 AND upper(trim(subq.acct_stat)) = 'UB' THEN coalesce(subq.eom_tot_acct_bal, NUMERIC '0')
                                                                                                                            WHEN subq.ins_order_num = 1 THEN CASE
                                                                                                                                                                 WHEN CASE subq.ins_plan1
                                                                                                                                                                          WHEN '' THEN 0
                                                                                                                                                                          ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                      END = 9940
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9941
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9942
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9943
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9944
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9945
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9946
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9947
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9948
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9949
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 9975
                                                                                                                                                                      AND (CASE subq.co_id
                                                                                                                                                                               WHEN '' THEN 0
                                                                                                                                                                               ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                           END = 2348
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 2560)
                                                                                                                                                                      OR CASE subq.ins_plan1
                                                                                                                                                                             WHEN '' THEN 0
                                                                                                                                                                             ELSE CAST(subq.ins_plan1 AS INT64)
                                                                                                                                                                         END = 95
                                                                                                                                                                      AND (CASE subq.co_id
                                                                                                                                                                               WHEN '' THEN 0
                                                                                                                                                                               ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                           END = 2531
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 1574
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 1578
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 1588
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 6676
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 6678
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 6679
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 196
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 6240
                                                                                                                                                                           OR CASE subq.co_id
                                                                                                                                                                                  WHEN '' THEN 0
                                                                                                                                                                                  ELSE CAST(subq.co_id AS INT64)
                                                                                                                                                                              END = 8224) THEN CAST(0 AS BIGNUMERIC)
                                                                                                                                                                 ELSE coalesce(subq.eom_pat_bal, NUMERIC '0')
                                                                                                                                                             END
                                                                                                                            ELSE CAST(0 AS BIGNUMERIC)
                                                                                                                        END) AS STRING)), ',', trim(CAST(sum(CASE
                                                                                                                                                                 WHEN subq.ins_order_num <> 0 THEN coalesce(subq.eom_ins_bal, NUMERIC '0')
                                                                                                                                                                 ELSE NUMERIC '0.00'
                                                                                                                                                             END) AS STRING)), ',', trim(CAST(sum(CASE
                                                                                                                                                                                                      WHEN subq.ins_order_num = 0
                                                                                                                                                                                                           AND CASE format_date('%Y%m', subq.disch_dt)
                                                                                                                                                                                                                   WHEN '' THEN 0
                                                                                                                                                                                                                   ELSE CAST(format_date('%Y%m', subq.disch_dt) AS INT64)
                                                                                                                                                                                                               END = CASE subq.rptg_period
                                                                                                                                                                                                                         WHEN '' THEN 0
                                                                                                                                                                                                                         ELSE CAST(subq.rptg_period AS INT64)
                                                                                                                                                                                                                     END THEN CAST(ROUND(CAST(coalesce(CAST(subq.eom_total_chgs AS FLOAT64), 0.0) AS BIGNUMERIC), 3, 'ROUND_HALF_EVEN') AS NUMERIC)
                                                                                                                                                                                                      ELSE NUMERIC '0.000'
                                                                                                                                                                                                  END) AS STRING)), ',', trim(CAST(sum(coalesce(subq.eom_all_rcvd, NUMERIC '0')) AS STRING)), ',') AS source_string
FROM
  (SELECT pc.rptg_period,
          pc.acct_stat,
          pc.ins_plan1,
          pc.co_id,
          parse_date('%Y-%m-%d', pc.disch_dt) AS disch_dt,
          CASE
              WHEN CASE pc.ins_plan1
                       WHEN '' THEN 0
                       ELSE CAST(pc.ins_plan1 AS INT64)
                   END <> 0 THEN CASE pc.eom_ins_bal1
                                     WHEN '' THEN NUMERIC '0'
                                     ELSE CAST(pc.eom_ins_bal1 AS NUMERIC)
                                 END
              ELSE NUMERIC '0.000'
          END AS eom_ins_bal,
          CASE pc.eom_ins_pymt_rcvd1
              WHEN '' THEN NUMERIC '0'
              ELSE CAST(pc.eom_ins_pymt_rcvd1 AS NUMERIC)
          END AS eom_pymt_rcvd,
          CASE pc.eom_ins_all_rcvd1
              WHEN '' THEN NUMERIC '0'
              ELSE CAST(pc.eom_ins_all_rcvd1 AS NUMERIC)
          END AS eom_all_rcvd,
          CASE pc.eom_pat_bal
              WHEN '' THEN NUMERIC '0'
              ELSE CAST(pc.eom_pat_bal AS NUMERIC)
          END AS eom_pat_bal,
          pc.eom_total_chgs,
          CASE pc.eom_tot_acct_bal
              WHEN '' THEN NUMERIC '0'
              ELSE CAST(pc.eom_tot_acct_bal AS NUMERIC)
          END AS eom_tot_acct_bal,
          1 AS ins_order_num
   FROM {{ params.param_pbs_stage_dataset_name }}.pass_current AS pc
   CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd
   WHERE upper(pc.co_id) = upper(fd.unit_num)
     AND CASE pc.ins_plan1
             WHEN '' THEN 0
             ELSE CAST(pc.ins_plan1 AS INT64)
         END <> 0
     AND pc.patient_dw_id IS NOT NULL
     AND pc.disch_dt IS NOT NULL
     AND (upper(fd.company_code) = 'H'
          OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
   UNION ALL SELECT pc.rptg_period,
                    pc.acct_stat,
                    '0' AS ins_plan1,
                    pc.co_id,
                    parse_date('%Y-%m-%d', pc.disch_dt) AS disch_dt,
                    CASE
                        WHEN CASE pc.ins_plan2
                                 WHEN '' THEN 0
                                 ELSE CAST(pc.ins_plan2 AS INT64)
                             END <> 0 THEN CASE pc.eom_ins_bal2
                                               WHEN '' THEN NUMERIC '0'
                                               ELSE CAST(pc.eom_ins_bal2 AS NUMERIC)
                                           END
                        ELSE NUMERIC '0.000'
                    END AS eom_ins_bal,
                    CASE pc.eom_ins_pymt_rcvd2
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_ins_pymt_rcvd2 AS NUMERIC)
                    END AS eom_pymt_rcvd,
                    CASE pc.eom_ins_all_rcvd2
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_ins_all_rcvd2 AS NUMERIC)
                    END AS eom_all_rcvd,
                    CASE pc.eom_pat_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_pat_bal AS NUMERIC)
                    END AS eom_pat_bal,
                    pc.eom_total_chgs,
                    CASE pc.eom_tot_acct_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_tot_acct_bal AS NUMERIC)
                    END AS eom_tot_acct_bal,
                    2 AS ins_order_num
   FROM {{ params.param_pbs_stage_dataset_name }}.pass_current AS pc
   CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd
   WHERE upper(pc.co_id) = upper(fd.unit_num)
     AND CASE pc.ins_plan2
             WHEN '' THEN 0
             ELSE CAST(pc.ins_plan2 AS INT64)
         END <> 0
     AND pc.patient_dw_id IS NOT NULL
     AND pc.disch_dt IS NOT NULL
     AND (upper(fd.company_code) = 'H'
          OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
   UNION ALL SELECT pc.rptg_period,
                    pc.acct_stat,
                    '0' AS ins_plan1,
                    pc.co_id,
                    parse_date('%Y-%m-%d', pc.disch_dt) AS disch_dt,
                    CASE
                        WHEN CASE pc.ins_plan3
                                 WHEN '' THEN 0
                                 ELSE CAST(pc.ins_plan3 AS INT64)
                             END <> 0 THEN CASE pc.eom_ins_bal3
                                               WHEN '' THEN NUMERIC '0'
                                               ELSE CAST(pc.eom_ins_bal3 AS NUMERIC)
                                           END
                        ELSE NUMERIC '0.000'
                    END AS eom_ins_bal,
                    CASE pc.eom_ins_pymt_rcvd3
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_ins_pymt_rcvd3 AS NUMERIC)
                    END AS eom_pymt_rcvd,
                    CASE pc.eom_ins_all_rcvd3
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_ins_all_rcvd3 AS NUMERIC)
                    END AS eom_all_rcvd,
                    CASE pc.eom_pat_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_pat_bal AS NUMERIC)
                    END AS eom_pat_bal,
                    pc.eom_total_chgs,
                    CASE pc.eom_tot_acct_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_tot_acct_bal AS NUMERIC)
                    END AS eom_tot_acct_bal,
                    3 AS ins_order_num
   FROM {{ params.param_pbs_stage_dataset_name }}.pass_current AS pc
   CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd
   WHERE upper(pc.co_id) = upper(fd.unit_num)
     AND CASE pc.ins_plan3
             WHEN '' THEN 0
             ELSE CAST(pc.ins_plan3 AS INT64)
         END <> 0
     AND pc.patient_dw_id IS NOT NULL
     AND pc.disch_dt IS NOT NULL
     AND (upper(fd.company_code) = 'H'
          OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y')
   UNION ALL SELECT pc.rptg_period,
                    pc.acct_stat,
                    '0' AS ins_plan1,
                    pc.co_id,
                    parse_date('%Y-%m-%d', pc.disch_dt) AS disch_dt,
                    CASE
                        WHEN CASE pc.ins_plan1
                                 WHEN '' THEN 0
                                 ELSE CAST(pc.ins_plan1 AS INT64)
                             END = 0 THEN CASE pc.eom_pat_bal
                                              WHEN '' THEN NUMERIC '0'
                                              ELSE CAST(pc.eom_pat_bal AS NUMERIC)
                                          END
                        ELSE NUMERIC '0.000'
                    END AS eom_ins_bal,
                    CASE pc.eom_pat_pymt_rcvd
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_pat_pymt_rcvd AS NUMERIC)
                    END AS eom_pymt_rcvd,
                    CASE pc.eom_pat_all_rcvd
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_pat_all_rcvd AS NUMERIC)
                    END AS eom_all_rcvd,
                    CASE pc.eom_pat_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_pat_bal AS NUMERIC)
                    END AS eom_pat_bal,
                    pc.eom_total_chgs,
                    CASE pc.eom_tot_acct_bal
                        WHEN '' THEN NUMERIC '0'
                        ELSE CAST(pc.eom_tot_acct_bal AS NUMERIC)
                    END AS eom_tot_acct_bal,
                    0 AS ins_order_num
   FROM {{ params.param_pbs_stage_dataset_name }}.pass_current AS pc
   CROSS JOIN {{ params.param_auth_base_views_dataset_name }}.facility_dimension AS fd
   WHERE upper(pc.co_id) = upper(fd.unit_num)
     AND pc.patient_dw_id IS NOT NULL
     AND pc.disch_dt IS NOT NULL
     AND (upper(fd.company_code) = 'H'
          OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y') ) AS subq