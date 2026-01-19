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

  SET sourcesysnm = 'metriq';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', '');

  SET tgtdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_core_dataset_name }}' , '.') as arr));

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
  (SELECT row_number() OVER (
                             ORDER BY sk.cancer_patient_driver_sk) AS cancer_patient_id_input_driver_sk,
                            sk.cancer_patient_driver_sk,
                            a.message_control_id_text,
                            a.patient_type_status_sk,
                            a.coid,
                            a.company_code,
                            a.patient_dw_id,
                            a.pat_acct_num,
                            a.medical_record_num,
                            a.patient_market_urn,
                            a.message_type_code,
                            a.message_flag_code,
                            a.message_event_type_code,
                            a.message_origin_requested_date_time,
                            a.message_signed_observation_date_time,
                            a.message_created_date_time,
                            a.first_insert_date_time,
                            'H' AS source_system_code,
                            datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_input AS a
   LEFT OUTER JOIN
     (SELECT a1.cancer_patient_driver_sk,
             a1.message_control_id_text
      FROM {{ params.param_cr_base_views_dataset_name }}.cancer_patient_id_output_driver AS a1 QUALIFY row_number() OVER (PARTITION BY upper(a1.message_control_id_text)
                                                                                                                          ORDER BY a1.user_action_date_time DESC) = 1) AS sk ON upper(rtrim(a.message_control_id_text)) = upper(rtrim(sk.message_control_id_text))) AS stg)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_id_input_driver)
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
      INSERT INTO {{ params.param_cr_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
