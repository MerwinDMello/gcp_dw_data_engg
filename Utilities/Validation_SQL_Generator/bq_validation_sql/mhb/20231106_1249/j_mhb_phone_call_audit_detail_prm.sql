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

  SET srcdataset_id = (select arr[offset(1)] from (select split({{ params.param_mhb_stage_dataset_name }} , '.') as arr));

  SET srctablename = concat(srcdataset_id, '.','' ); -- This needs to be added

  SET tgtdataset_id = (select arr[offset(1)] from (select split({{ params.param_mhb_core_dataset_name }} , '.') as arr));

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
  (SELECT rdc.rdc_sid
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwuserphonecalls AS pc
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(rdc.rdc_desc) = upper(replace(pc.databasename, 'heartbeatDW_', ''))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS mu
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(pc.user_name) = upper(mu.user_login_name)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS ur
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(pc.user_role)) = upper(trim(ur.mhb_user_role_desc))
   AND ur.mhb_user_role_sid = mu.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(pc.facilitycode)
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl.location_mnemonic_cs) = trim(substr(pc.unitcode, strpos(pc.unitcode, '_') + 1))
   WHERE mu.eff_to_date = DATE '9999-12-31'
     AND upper(mu.active_dw_ind) = 'Y' ) AS q)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_phone_call_audit_detail.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_phone_call_audit_detail
   WHERE mhb_phone_call_audit_detail.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q)
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
      INSERT INTO {{ params.param_mhb_audit_dataset_name }}.audit_control
      VALUES
      (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
      expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
      tableload_run_time, job_name, audit_time, audit_status
      );

    END LOOP;

END;
