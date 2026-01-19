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
FROM (SELECT concat(trim(format('%20d', coalesce(count(k.pat_acct_num), 0))), trim(CAST(sum(CAST(k.discharge_to_billing_day_cnt AS NUMERIC)) AS STRING))) AS source_string
FROM
  (SELECT a.pat_acct_num,
          CASE format_date('%Y%m', date_sub(current_date('US/Central'), interval 1 MONTH))
              WHEN '' THEN 0
              ELSE CAST(format_date('%Y%m', date_sub(current_date('US/Central'), interval 1 MONTH)) AS INT64)
          END AS date_sid, date_diff(a.final_bill_date, a.discharge_date, DAY) AS discharge_to_billing_day_cnt,
                           max(a.company_code) AS company_code,
                           max(a.coid) AS coid,
                           CASE ff.unit_num
                               WHEN '' THEN 0
                               ELSE CAST(ff.unit_num AS INT64)
                           END AS unitnum,
                           CASE
                               WHEN fp.account_type_sid IS NULL THEN 1
                               ELSE fp.account_type_sid
                           END AS account_type_sid,
                           CASE
                               WHEN fp.account_status_sid IS NULL THEN 10
                               ELSE fp.account_status_sid
                           END AS account_status_sid,
                           CASE
                               WHEN fp.age_month_sid IS NULL THEN 1
                               ELSE fp.age_month_sid
                           END AS age_month_sid,
                           CASE
                               WHEN fp.patient_financial_class_sid IS NULL THEN 23
                               ELSE fp.patient_financial_class_sid
                           END AS patient_financial_class_sid,
                           CASE
                               WHEN fp.patient_type_sid IS NULL THEN 40
                               ELSE fp.patient_type_sid
                           END AS patient_type_sid,
                           CASE
                               WHEN fp.collection_agency_sid IS NULL THEN 1
                               ELSE fp.collection_agency_sid
                           END AS collection_agency_sid,
                           CASE
                               WHEN fp.payor_sid IS NULL THEN 1
                               ELSE fp.payor_sid
                           END AS payor_sid,
                           CASE
                               WHEN fp.payor_financial_class_sid IS NULL THEN 24
                               ELSE fp.payor_financial_class_sid
                           END AS payor_financial_class_sid,
                           CASE
                               WHEN fp.product_sid IS NULL THEN 22
                               ELSE fp.product_sid
                           END AS product_sid,
                           CASE
                               WHEN fp.contract_sid IS NULL THEN 1
                               ELSE fp.contract_sid
                           END AS contract_sid,
                           CASE
                               WHEN fp.scenario_sid IS NULL THEN 1
                               ELSE fp.scenario_sid
                           END AS scenario_sid,
                           CASE
                               WHEN fp.source_sid IS NULL THEN 1
                               ELSE fp.source_sid
                           END AS source_sid,
                           0 AS iplan_insurance_order_num,
                           1 AS billed_patient_cnt
   FROM {{ params.param_auth_base_views_dataset_name }}.fact_patient AS a
   INNER JOIN
     (SELECT y.patient_dw_id,
             max(y.service_code_start_date) AS service_code_start_date
      FROM {{ params.param_auth_base_views_dataset_name }}.registration_service AS y
      GROUP BY 1) AS z ON z.patient_dw_id = a.patient_dw_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.registration_service AS b ON b.patient_dw_id = a.patient_dw_id
   AND b.service_code_start_date = z.service_code_start_date
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.ref_service AS c ON upper(b.coid) = upper(c.coid)
   AND upper(b.service_code) = upper(c.service_code)
   LEFT OUTER JOIN {{ params.param_pf_stage_dataset_name }}.pass_current AS pc ON a.patient_dw_id = pc.patient_dw_id
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_ar_patient_level AS fp
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON a.pat_acct_num = fp.patient_sid
   AND upper(fp.company_code) = upper(a.company_code)
   AND upper(fp.coid) = upper(a.coid)
   AND fp.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                         WHEN '' THEN 0
                         ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                     END
   AND fp.iplan_insurance_order_num = 0
   AND fp.age_month_sid = 20
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(ff.coid) = upper(a.coid)
   AND upper(ff.company_code) = upper(a.company_code)
   WHERE (upper(a.company_code) = 'H'
          OR upper(substr(trim(ff.company_code_operations), 1, 1)) = 'Y')
     AND CASE
             WHEN pc.eom_total_chgs IS NOT NULL
                  AND CASE pc.eom_total_chgs
                          WHEN '' THEN NUMERIC '0'
                          ELSE CAST(pc.eom_total_chgs AS NUMERIC)
                      END <> 0 THEN ROUND(CASE pc.eom_total_chgs
                                              WHEN '' THEN NUMERIC '0'
                                              ELSE CAST(pc.eom_total_chgs AS NUMERIC)
                                          END / 100, 3, 'ROUND_HALF_EVEN')
             ELSE a.total_billed_charges
         END <> 0
     AND upper(a.patient_type_code_pos1) = 'I'
     AND upper(c.service_exempt_flag) = 'N'
     AND (a.discharge_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_sub(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 1 DAY)
          AND a.final_bill_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH) AND date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY)
          OR a.discharge_date <= date_sub(date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval -1 MONTH), interval 1 DAY)
          AND a.final_bill_date BETWEEN date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -1 MONTH)), '-01')) AS DATE), interval 4 DAY) AND date_add(CAST(trim(concat(format_date('%Y-%m', date_add(current_date('US/Central'), interval -0 MONTH)), '-01')) AS DATE), interval 3 DAY))
     AND b.service_code_start_date <= a.discharge_date
   GROUP BY 1,
            2,
            3,
            upper(a.company_code),
            upper(a.coid),
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20) AS k ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(trim(format('%11d', coalesce(sum(fact_rcom_ar_patient_level.billed_patient_cnt), 0))), trim(CAST(sum(CAST(fact_rcom_ar_patient_level.discharge_to_billing_day_cnt AS NUMERIC)) AS STRING))) AS source_string
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
