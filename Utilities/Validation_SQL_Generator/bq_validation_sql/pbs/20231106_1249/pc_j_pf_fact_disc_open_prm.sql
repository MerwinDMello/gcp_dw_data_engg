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
FROM (SELECT concat('0', trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(CAST(coalesce(sum(abs(rd.var_gross_reimbursement_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_contractual_allowance_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_insurance_payment_amt)), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(abs(rd.var_net_billed_charge_amt)), CAST(0 AS BIGNUMERIC)) AS STRING))) AS source_string
FROM {{ params.param_auth_base_views_dataset_name }}.reimbursement_discrepancy_eom AS rd
LEFT OUTER JOIN {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS eor ON upper(rd.company_code) = upper(eor.company_code)
AND upper(rd.coid) = upper(eor.coid)
AND rd.patient_dw_id = eor.patient_dw_id
AND rd.payor_dw_id = eor.payor_dw_id
AND rd.iplan_insurance_order_num = eor.iplan_insurance_order_num
AND rd.eor_log_date = eor.eor_log_date
AND upper(rd.log_id) = upper(eor.log_id)
AND rd.log_sequence_num = eor.log_sequence_num
AND coalesce(rd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(eor.cc_calc_id, CAST(999 AS BIGNUMERIC))
WHERE eor.eff_from_date =
    (SELECT max(er.eff_from_date)
     FROM {{ params.param_auth_base_views_dataset_name }}.explanation_of_reimbursement AS er
     WHERE upper(rd.company_code) = upper(er.company_code)
       AND upper(rd.coid) = upper(er.coid)
       AND rd.patient_dw_id = er.patient_dw_id
       AND rd.payor_dw_id = er.payor_dw_id
       AND rd.iplan_insurance_order_num = er.iplan_insurance_order_num
       AND rd.eor_log_date = er.eor_log_date
       AND upper(rd.log_id) = upper(er.log_id)
       AND rd.log_sequence_num = er.log_sequence_num
       AND coalesce(rd.cc_calc_id, CAST(999 AS BIGNUMERIC)) = coalesce(er.cc_calc_id, CAST(999 AS BIGNUMERIC))
       AND er.eff_from_date <= date_sub(CAST(trim(concat(format_date('%Y-%m', current_date('US/Central')), '-01')) AS DATE), interval 1 DAY) )
  AND upper(rd.coid) NOT IN
    (SELECT upper(parallon_client_detail.coid) AS coid
     FROM {{ params.param_pbs_core_dataset_name }}.parallon_client_detail
     FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')
     WHERE upper(parallon_client_detail.company_code) = 'CHP' ) ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT concat('0', trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(format('%4d', coalesce(sum(0), 0))), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_gross_rbmt_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_cont_alw_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_payment_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING)), trim(CAST(coalesce(sum(fact_rcom_pars_discrepancy.var_charge_end_amt), CAST(0 AS BIGNUMERIC)) AS STRING))) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.fact_rcom_pars_discrepancy
WHERE fact_rcom_pars_discrepancy.date_sid = CASE format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH))
                                                WHEN '' THEN 0
                                                ELSE CAST(format_date('%Y%m', date_add(current_date('US/Central'), interval -1 MONTH)) AS INT64)
                                            END
  AND fact_rcom_pars_discrepancy.discrepancy_resolved_date = DATE '0001-01-01'
  AND fact_rcom_pars_discrepancy.patient_sid <> NUMERIC '999999999999'
  AND upper(fact_rcom_pars_discrepancy.coid) NOT IN
    (SELECT upper(parallon_client_detail.coid) AS coid
     FROM {{ params.param_pbs_core_dataset_name }}.parallon_client_detail
     WHERE upper(parallon_client_detail.company_code) = 'CHP' ) ;)
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
