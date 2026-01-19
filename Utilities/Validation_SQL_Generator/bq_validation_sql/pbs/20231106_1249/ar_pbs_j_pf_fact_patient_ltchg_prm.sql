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
FROM (SELECT concat(exp1.late_charge_credit_amt, exp1.late_charge_debit_amt) AS source_string
FROM
  (SELECT date_sub(date_add(CAST(trim(concat(format_date('%Y-%m', psc.charge_posted_date), '-01')) AS DATE), interval 1 MONTH), interval 6 DAY) AS date1,
          ERROR_UNIMPLEMENTED(sum(CASE
                                      WHEN psc.charge_amt < 0 THEN psc.charge_amt
                                      ELSE NUMERIC '0.00'
                                  END)) AS late_charge_credit_amt,
          ERROR_UNIMPLEMENTED(sum(CASE
                                      WHEN psc.charge_amt >= 0 THEN psc.charge_amt
                                      ELSE NUMERIC '0.00'
                                  END)) AS late_charge_debit_amt
   FROM {{ params.param_auth_base_views_dataset_name }}.patient_suspended_charge AS psc
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.fact_facility AS ff ON upper(ff.coid) = upper(psc.coid)
   AND upper(ff.company_code) = upper(psc.company_code)
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.fact_rcom_ar_patient_level AS fp
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON fp.patient_sid = psc.pat_acct_num
   AND upper(fp.company_code) = upper(psc.company_code)
   AND upper(fp.coid) = upper(psc.coid)
   AND CASE format_date('%Y%m', psc.charge_posted_date)
           WHEN '' THEN 0
           ELSE CAST(format_date('%Y%m', psc.charge_posted_date) AS INT64)
       END = fp.date_sid
   AND fp.iplan_insurance_order_num = 0
   WHERE upper(format_date('%Y%m', psc.charge_posted_date)) = upper(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)))
     AND upper(psc.source_system_code) = 'P'
     AND (upper(psc.company_code) = 'H'
          OR upper(substr(trim(ff.company_code_operations), 1, 1)) = 'Y')
   GROUP BY 1) AS exp1 ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat(coalesce(CAST(sum(fact_rcom_ar_patient_level.late_charge_credit_amt) AS STRING), '0'), coalesce(CAST(sum(fact_rcom_ar_patient_level.late_charge_debit_amt) AS STRING), '0')) AS source_string
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
