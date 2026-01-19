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
  (SELECT
     (SELECT coalesce(max(ref_mhb_dynamic_role.dynamic_role_sid), 0)
      FROM {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_dynamic_role
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY y.rdc_sid,
                                                                                                         upper(y.dynamicrole_name),
                                                                                                         upper(y.dynamicrole_label)) AS dynamic_role_sid,
                                                                                               y.rdc_sid AS rdc_sid,
                                                                                               y.dynamicrole_name AS mhb_dynamic_role_parent_name,
                                                                                               y.dynamicrole_label AS mhb_dynamic_role_child_name,
                                                                                               'B' AS source_system_code,
                                                                                               timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT vwdynamicrole.dynamicrole_name,
             vwdynamicrole.dynamicrole_label,
             rdc.rdc_sid
      FROM
        (SELECT max(vwdynamicroledetachment.dynamicrole_name) AS dynamicrole_name,
                max(vwdynamicroledetachment.dynamicrole_label) AS dynamicrole_label,
                max(vwdynamicroledetachment.databasename) AS databasename
         FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwdynamicroledetachment
         GROUP BY upper(vwdynamicroledetachment.dynamicrole_name),
                  upper(vwdynamicroledetachment.dynamicrole_label),
                  upper(vwdynamicroledetachment.databasename)
         UNION DISTINCT SELECT substr(max(vwdynamicroleattachment.dynamicrole_name), 1, 30) AS dynamicrole_name,
                               substr(max(vwdynamicroleattachment.dynamicrole_label), 1, 40) AS dynamicrole_label,
                               max(vwdynamicroleattachment.databasename) AS databasename
         FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwdynamicroleattachment
         GROUP BY upper(substr(vwdynamicroleattachment.dynamicrole_name, 1, 30)),
                  upper(substr(vwdynamicroleattachment.dynamicrole_label, 1, 40)),
                  upper(vwdynamicroleattachment.databasename)) AS vwdynamicrole
      INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(vwdynamicrole.databasename), strpos(trim(vwdynamicrole.databasename), '_') + 1)) = upper(rdc.rdc_desc)
      LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_dynamic_role AS dr
      FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(vwdynamicrole.dynamicrole_name) = upper(dr.mhb_dynamic_role_parent_name)
      AND upper(vwdynamicrole.dynamicrole_label) = upper(dr.mhb_dynamic_role_child_name)
      AND rdc.rdc_sid = dr.rdc_sid
      WHERE dr.dynamic_role_sid IS NULL ) AS y) AS q)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT ref_mhb_dynamic_role.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.ref_mhb_dynamic_role
   WHERE ref_mhb_dynamic_role.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q)
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
