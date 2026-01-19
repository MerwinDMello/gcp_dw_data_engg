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
FROM (SELECT trim(format('%20d', coalesce(count(*), 0))) AS source_string
FROM
  (SELECT fp.patient_sid,
          fp.account_type_sid,
          fp.account_status_sid,
          fp.age_month_sid,
          fp.patient_financial_class_sid,
          fp.patient_type_sid,
          fp.collection_agency_sid,
          fp.payor_financial_class_sid,
          fp.product_sid,
          fp.contract_sid,
          fp.scenario_sid,
          fp.date_sid,
          fp.source_sid,
          fp.unit_num_sid,
          fp.iplan_insurance_order_num,
          fp.coid,
          fp.company_code,
          fp.patient_account_cnt,
          fp.discharge_cnt,
          fp.ar_patient_amt,
          fp.ar_insurance_amt,
          fp.write_off_amt,
          fp.total_collect_amt,
          fp.billed_patient_cnt,
          fp.discharge_to_billing_day_cnt,
          fp.gross_charge_amt,
          fp.prorated_liability_sys_adj_amt,
          fp.late_charge_credit_amt,
          fp.late_charge_debit_amt,
          fp.payor_prorated_liability_amt,
          fp.payor_payment_amt,
          fp.payor_adjustment_amt,
          fp.payor_contractual_amt,
          fp.payor_denial_amt,
          fp.payor_denial_cnt,
          fp.payor_expected_payment_amt,
          fp.payor_discrepancy_ovr_pmt_amt,
          fp.payor_discrepancy_undr_pmt_amt,
          fp.payor_up_front_collection_amt,
          fp.payor_bill_cnt,
          fp.payor_rebill_cnt,
          fp.payor_sid,
          fp.unbilled_gross_bus_ofc_amt,
          fp.unbilled_gross_med_rec_amt
   FROM {{ params.param_auth_base_views_dataset_name }}.fact_rcom_ar_patient_level AS fp
   WHERE fp.date_sid =
       (SELECT max(eis_date_dim.time_id)
        FROM {{ params.param_auth_base_views_dataset_name }}.eis_date_dim
        WHERE upper(eis_date_dim.current_mth) = 'Y' ) ) AS p ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT trim(format('%20d', coalesce(count(*), 0))) AS source_string
FROM {{ params.param_auth_base_views_dataset_name }}.fact_rcom_ar_patient_lvl_cm ;)
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
    SET audit_type = CONCAT("INGEST_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO "{{ params.param_pbs_audit_dataset_name }}".audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, job_name, audit_time, audit_status
   );

END LOOP;
