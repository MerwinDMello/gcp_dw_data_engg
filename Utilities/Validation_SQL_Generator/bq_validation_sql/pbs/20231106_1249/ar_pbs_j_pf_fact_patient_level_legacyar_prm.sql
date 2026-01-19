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
FROM (SELECT concat('PBMAR100', ',', trim(CAST(sum(coalesce(subq.eom_pymt_rcvd, NUMERIC '0')) AS STRING)), ',', trim(CAST(sum(CASE
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
          OR upper(substr(trim(fd.company_code_operations), 1, 1)) = 'Y') ) AS subq ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(CAST(coalesce(sum(arp.payor_payment_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.ar_patient_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.ar_insurance_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.gross_charge_amt), NUMERIC '0') AS STRING), CAST(coalesce(sum(arp.payor_contractual_amt), NUMERIC '0') AS STRING)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_ar_patient_level AS arp
WHERE arp.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                         WHEN '' THEN 0.0
                         ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS FLOAT64)
                     END
  AND arp.patient_sid <> 0
  AND arp.source_sid = 9
  AND upper(arp.coid) IN
    (SELECT upper(max(pass_current.co_id)) AS co_id
     FROM {{ params.param_pbs_stage_dataset_name }}.pass_current
     GROUP BY upper(pass_current.co_id)) ;)
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
