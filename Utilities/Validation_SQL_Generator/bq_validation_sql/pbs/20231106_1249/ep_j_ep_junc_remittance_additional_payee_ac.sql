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
FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT f.payment_guid,
          f.additional_payee_line_num,
          rrap.remittance_additional_payee_sid AS remittance_additional_payee_sid, 'E' AS source_system_code,
                                                                                   timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_payment.payment_guid,
             remittance_payment.additional_payee1_id AS additional_payee_id,
             remittance_payment.reference_id_qualifier1_code AS additional_payee_id_qualifier_code,
             1 AS additional_payee_line_num
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment
      WHERE upper(coalesce(remittance_payment.reference_id_qualifier1_code, '')) <> ''
        OR upper(coalesce(remittance_payment.additional_payee1_id, '')) <> ''
      UNION DISTINCT SELECT remittance_payment.payment_guid,
                            remittance_payment.additional_payee2_id AS additional_payee_id,
                            remittance_payment.reference_id_qualifier2_code AS additional_payee_id_qualifier_code,
                            2 AS additional_payee_line_num
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment
      WHERE upper(coalesce(remittance_payment.reference_id_qualifier2_code, '')) <> ''
        OR upper(coalesce(remittance_payment.additional_payee2_id, '')) <> ''
      UNION DISTINCT SELECT remittance_payment.payment_guid,
                            remittance_payment.additional_payee3_id AS additional_payee_id,
                            remittance_payment.reference_id_qualifier3_code AS additional_payee_id_qualifier_code,
                            3 AS additional_payee_line_num
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment
      WHERE upper(coalesce(remittance_payment.reference_id_qualifier3_code, '')) <> ''
        OR upper(coalesce(remittance_payment.additional_payee3_id, '')) <> ''
      UNION DISTINCT SELECT remittance_payment.payment_guid,
                            remittance_payment.additional_payee4_id AS additional_payee_id,
                            remittance_payment.reference_id_qualifier4_code AS additional_payee_id_qualifier_code,
                            4 AS additional_payee_line_num
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_payment
      WHERE upper(coalesce(remittance_payment.reference_id_qualifier4_code, '')) <> ''
        OR upper(coalesce(remittance_payment.additional_payee4_id, '')) <> '' ) AS f
   LEFT OUTER JOIN {{ params.param_pbs_core_dataset_name }}.ref_remittance_additional_payee AS rrap
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(f.additional_payee_id) = upper(rrap.additional_payee_id)
   AND upper(f.additional_payee_id_qualifier_code) = upper(rrap.additional_payee_id_qualifier_code)) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.junc_remittance_additional_payee
WHERE junc_remittance_additional_payee.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
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
