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

  SET sourcesysnm = 'navadhoc';

  SET srcdataset_id = (select arr[offset(1)] from (select split('{{ params.param_cr_stage_dataset_name }}' , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.', 'cn_physician_role_stg');

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
  (SELECT cn_physician_role_stg.physician_id,
          cn_physician_role_stg.physician_role_code
   FROM {{ params.param_cr_stage_dataset_name }}.cn_physician_role_stg
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'Gyn' AS physician_role_code
   FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
   INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cn_physician_detail.physician_id = cn_patient.gynecologist_physician_id
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'PCP' AS physician_role_code
   FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
   INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cn_physician_detail.physician_id = cn_patient.primary_care_physician_id
   UNION DISTINCT SELECT cn_physician_detail.physician_id,
                         'ETP' AS physician_role_code
   FROM {{ params.param_cr_core_dataset_name }}.cn_physician_detail
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')
   INNER JOIN {{ params.param_cr_core_dataset_name }}.cn_patient_tumor
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON cn_physician_detail.physician_id = cn_patient_tumor.treatment_end_physician_id) AS ab
WHERE (ab.physician_id,
       upper(ab.physician_role_code)) NOT IN
    (SELECT AS STRUCT cn_physician_role.physician_id,
                      upper(cn_physician_role.physician_role_code) AS physician_role_code
     FROM {{ params.param_cr_core_dataset_name }}.cn_physician_role
     FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')))
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_cr_core_dataset_name }}.cn_physician_role
WHERE cn_physician_role.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE)
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
