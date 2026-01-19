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
  (SELECT rdc.rdc_sid,
          stg.message_id,
          max(coalesce(cf.company_code, 'H')) AS company_code,
          max(coalesce(cf.coid, '99999')) AS coid,
          max(u.user_login_name) AS user_login_name,
          max(coalesce(cf.coid, '99999')) AS recipient_coid,
          coalesce(cfl.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          rmu2.mhb_unit_id AS recipient_mhb_unit_id,
          r.mhb_user_role_sid AS recipient_mhb_user_role_sid,
          max(stg.sender_information) AS sender_information,
          max(stg.sender_message_id) AS sender_message_id,
          max(stg.sender_transaction_id) AS sender_transaction_id,
          CAST(trim(stg.received_date_time) AS DATETIME) AS message_received_date_time,
          CAST(trim(stg.sent_date_time) AS DATETIME) AS message_sent_date_time,
          CASE
              WHEN upper(trim(stg.delivered_date_time)) = '' THEN CAST(NULL AS DATETIME)
              ELSE CAST(trim(stg.delivered_date_time) AS DATETIME)
          END AS message_delivered_date_time,
          CASE
              WHEN upper(trim(stg.read_date_time)) = '' THEN CAST(NULL AS DATETIME)
              ELSE CAST(trim(stg.read_date_time) AS DATETIME)
          END AS message_read_date_time,
          stg.message_status,
          stg.message_parse_status,
          coalesce(mdp.message_delivery_priority_sid, -1) AS message_delivery_priority_sid,
          max(stg.message_payload) AS message_payload_text,
          coalesce(sn.wctp_source_name_sid, -1) AS wctp_source_name_sid,
          coalesce(st.wctp_source_type_sid, -1) AS wctp_source_type_sid,
          coalesce(uat.user_action_type_sid, -1) AS user_action_type_sid,
          stg.notify_when_queued AS notify_when_queued_sw,
          stg.notify_when_delivered AS notify_when_delivered_sw,
          stg.notify_when_read AS notify_when_read_sw,
          stg.inferredupdate AS inferred_update_num,
          max(stg.lasttimestamp) AS mhb_last_update_date_time,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwwctpinboundmessages AS stg
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.recipient_username) = upper(u.user_login_name)
   AND rdc.rdc_sid = u.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.recipient_role), 'Unknown')) = upper(trim(r.mhb_user_role_desc))
   AND r.mhb_user_role_sid = u.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl.location_mnemonic_cs) = trim(substr(stg.recipient_unit_code, strpos(stg.recipient_unit_code, '_') + 1))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND stg.recipient_unit_id = rmu2.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_message_delivery_priority AS mdp
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.message_delivery_priority, 'Unknown')) = upper(mdp.message_delivery_priority_desc)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_wctp_source_name AS sn
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.source_name, 'Unknown')) = upper(sn.wctp_source_name)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_wctp_source_type AS st
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.source_type, 'Unknown')) = upper(st.wctp_source_type_desc)
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_action_type AS uat
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(stg.user_action, 'Unknown')) = upper(uat.user_action_type_desc)
   WHERE CAST(trim(substr(stg.received_date_time, 1, 10)) AS DATE) BETWEEN u.eff_from_date AND u.eff_to_date
   GROUP BY 1,
            2,
            upper(coalesce(cf.company_code, 'H')),
            upper(coalesce(cf.coid, '99999')),
            upper(u.user_login_name),
            upper(coalesce(cf.coid, '99999')),
            7,
            8,
            9,
            upper(stg.sender_information),
            upper(stg.sender_message_id),
            upper(stg.sender_transaction_id),
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            upper(stg.message_payload),
            21,
            22,
            23,
            24,
            25,
            26,
            27,
            upper(stg.lasttimestamp),
            29,
            30) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_wctp_inbound_message.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_wctp_inbound_message
   WHERE mhb_wctp_inbound_message.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q)
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
