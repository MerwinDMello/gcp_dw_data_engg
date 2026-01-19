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
FROM (SELECT concat(coalesce(regexp_replace(format('%#20.3f', exp1.sum1), r'^(.*?)(-)?0(\..*)', r'\1 \2\3'), '0'), coalesce(format('%4d', exp1.cnt1), '0')) AS source_string
FROM
  (SELECT k.pe_date,
          coalesce(CAST(ROUND(CAST(sum(CASE k.w_off_denial_account_amt
                                           WHEN '' THEN 0.0
                                           ELSE CAST(k.w_off_denial_account_amt AS FLOAT64)
                                       END) AS BIGNUMERIC), 3, 'ROUND_HALF_EVEN') AS NUMERIC), CAST(0 AS NUMERIC)) AS sum1,
          coalesce(sum(CASE
                           WHEN CASE k.w_off_denial_account_amt
                                    WHEN '' THEN 0.0
                                    ELSE CAST(k.w_off_denial_account_amt AS FLOAT64)
                                END <> 0 THEN 1
                           ELSE 0
                       END), 0) AS cnt1
   FROM
     (SELECT k_0.pat_acct_num,
             k_0.pe_date,
             k_0.iplan_insurance_order_num,
             k_0.company_code AS k_company_code,
             k_0.coid AS k_coid,
             k_0.w_off_denial_account_amt,
             k_0.admission_date,
             k_0.patient_type_sid,
             k_0.unit_num_sid,
             pd.payor_sid,
             k_0.patient_financial_class_sid,
             k_0.payor_financial_class_sid,
             fp.coid,
             fp.company_code,
             fp.account_type_sid,
             fp.account_status_sid,
             fp.age_month_sid,
             fp.collection_agency_sid,
             fp.product_sid,
             fp.contract_sid,
             fp.scenario_sid
      FROM
        (SELECT deom.pat_acct_num,
                deom.pe_date,
                deom.iplan_insurance_order_num,
                deom.company_code,
                deom.coid, w_off_denial_account_amt AS w_off_denial_account_amt,
                           adm.admission_date,
                           ptd.patient_type_sid,
                           deom.unit_num AS unit_num_sid,
                           CASE
                               WHEN rpdeb.payor_id IS NOT NULL THEN trim(rpdeb.payor_id)
                               WHEN rpde.payor_id IS NOT NULL THEN trim(rpde.payor_id)
                           END AS payor_id,
                           CASE deom.patient_financial_class_code
                               WHEN 99 THEN 21
                               WHEN 15 THEN 18
                               WHEN 13 THEN 5
                               WHEN 4 THEN 9
                               WHEN 11 THEN 15
                               WHEN 7 THEN 4
                               WHEN 0 THEN 22
                               WHEN 1 THEN 1
                               WHEN 10 THEN 14
                               WHEN 14 THEN 17
                               WHEN 12 THEN 16
                               WHEN 3 THEN 3
                               WHEN 9 THEN 12
                               WHEN 6 THEN 11
                               WHEN 8 THEN 6
                               WHEN 2 THEN 2
                               WHEN 5 THEN 10
                               ELSE 23
                           END AS patient_financial_class_sid,
                           CASE deom.payor_financial_class_code
                               WHEN 0 THEN 23
                               WHEN 1 THEN 1
                               WHEN 2 THEN 2
                               WHEN 3 THEN 3
                               WHEN 4 THEN 9
                               WHEN 5 THEN 10
                               WHEN 6 THEN 11
                               WHEN 7 THEN 4
                               WHEN 8 THEN 6
                               WHEN 9 THEN 12
                               WHEN 10 THEN 14
                               WHEN 11 THEN 15
                               WHEN 12 THEN 16
                               WHEN 13 THEN 5
                               WHEN 14 THEN 17
                               WHEN 15 THEN 18
                               WHEN 20 THEN 20
                               WHEN 99 THEN 20
                               ELSE 24
                           END AS payor_financial_class_sid
         FROM {{ params.param_auth_base_views_dataset_name }}.denial_eom AS deom
         LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.admission AS adm ON adm.patient_dw_id = deom.patient_dw_id
         INNER JOIN {{ params.param_auth_base_views_dataset_name }}.eis_patient_type_dim AS ptd ON upper(concat('PT_', deom.patient_type_code)) = upper(ptd.patient_type_member)
         AND upper(ptd.patient_type_gen02) NOT LIKE 'MC%'
         LEFT OUTER JOIN
           (SELECT rcom_payor_dimension_eom.payor_dw_id,
                   rcom_payor_dimension_eom.payor_id,
                   rcom_payor_dimension_eom.eff_from_date,
                   rcom_payor_dimension_eom.eff_to_date
            FROM {{ params.param_pbs_base_views_dataset_name }}.rcom_payor_dimension_eom
            FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) AS rpdeb ON deom.payor_dw_id = rpdeb.payor_dw_id
         AND adm.admission_date BETWEEN rpdeb.eff_from_date AND rpdeb.eff_to_date
         LEFT OUTER JOIN
           (SELECT rcom_payor_dimension_eom.payor_dw_id,
                   rcom_payor_dimension_eom.payor_id,
                   rcom_payor_dimension_eom.eff_from_date
            FROM {{ params.param_pbs_base_views_dataset_name }}.rcom_payor_dimension_eom
            FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) AS rpde ON deom.payor_dw_id = rpde.payor_dw_id
         AND rpde.eff_from_date IS NULL
         CROSS JOIN UNNEST(ARRAY[ substr(ltrim(regexp_replace(format('%#20.3f', CASE
                                                                                    WHEN upper(deom.source_system_code) = 'N' THEN deom.cc_cash_adjustment_amt
                                                                                    ELSE deom.write_off_denial_account_amt
                                                                                END), r'^( *?)(-)?0(\..*)', r'\2\3')), 1, 22) ]) AS w_off_denial_account_amt
         WHERE CASE w_off_denial_account_amt
                   WHEN '' THEN 0.0
                   ELSE CAST(w_off_denial_account_amt AS FLOAT64)
               END <> 0
           AND upper(format_date('%Y%m', deom.pe_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))) ) AS k_0
      LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.eis_payor_dim AS pd
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(pd.payor_member) = upper(k_0.payor_id)
      LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_ar_patient_level AS fp
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON fp.patient_sid = k_0.pat_acct_num
      AND upper(fp.company_code) = upper(k_0.company_code)
      AND upper(fp.coid) = upper(k_0.coid)
      AND CASE format_date('%Y%m', k_0.pe_date)
              WHEN '' THEN 0
              ELSE CAST(format_date('%Y%m', k_0.pe_date) AS INT64)
          END = fp.date_sid
      AND fp.iplan_insurance_order_num = k_0.iplan_insurance_order_num
      AND fp.payor_sid = pd.payor_sid
      AND fp.unit_num_sid = CASE k_0.unit_num_sid
                                WHEN '' THEN 0.0
                                ELSE CAST(k_0.unit_num_sid AS FLOAT64)
                            END
      AND fp.patient_type_sid = k_0.patient_type_sid
      AND fp.patient_financial_class_sid = k_0.patient_financial_class_sid
      AND fp.payor_financial_class_sid = k_0.payor_financial_class_sid) AS k
   GROUP BY 1) AS exp1 ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(coalesce(CAST(sum(fact_rcom_ar_patient_level.payor_denial_amt) AS STRING), '0'), coalesce(format('%11d', sum(fact_rcom_ar_patient_level.payor_denial_cnt)), '0')) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_ar_patient_level
WHERE fact_rcom_ar_patient_level.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                WHEN '' THEN 0
                                                ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                                            END ;)
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
