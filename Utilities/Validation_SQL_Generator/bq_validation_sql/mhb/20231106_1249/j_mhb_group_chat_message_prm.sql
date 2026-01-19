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
          max(u10.user_login_name) AS recipient_user_login_name,
          stg.messages_id AS mhb_group_chat_message_id,
          stg.sent_date_time AS message_sent_date_time,
          coalesce(ck.patient_dw_id, NUMERIC '999999999999999999') AS patient_dw_id,
          max(coalesce(ck.company_code, 'H')) AS company_code,
          max(coalesce(ck.coid, '99999')) AS coid,
          max(u20.user_login_name) AS sender_user_login_name,
          rmu2.mhb_unit_id AS sender_mhb_unit_id,
          max(coalesce(cf22.coid, '99999')) AS sender_coid,
          coalesce(cfl5.location_mnemonic_cs, 'Unknown') AS sender_location_mnemonic_cs,
          u20.mhb_user_role_sid AS sender_mhb_user_role_sid,
          max(stg.sender_platform) AS sender_platform_name,
          max(stg.group_chat_name) AS group_chat_name,
          max(stg.group_chat_displayname) AS group_chat_display_name,
          max(CASE upper(stg.quickpick)
                  WHEN 'TRUE' THEN 'Y'
                  WHEN 'FALSE' THEN 'N'
              END) AS quick_pick_ind,
          max(CASE upper(stg.urgent)
                  WHEN 'TRUE' THEN 'Y'
                  WHEN 'FALSE' THEN 'N'
              END) AS urgent_message_ind,
          ROUND(CASE trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999'))
                    WHEN '' THEN NUMERIC '0'
                    ELSE CAST(trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999')) AS NUMERIC)
                END, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
          max(coalesce(cf41.coid, '99999')) AS patient_coid,
          u10.mhb_user_role_sid AS recipient_mhb_user_role_sid,
          rmu51.mhb_unit_id AS recipient_mhb_unit_id,
          coalesce(cfl2.location_mnemonic_cs, 'Unknown') AS recipient_location_mnemonic_cs,
          max(coalesce(cf53.coid, '99999')) AS recipient_coid,
          stg.inferredupdate AS inferred_update_sw,
          stg.lasttimestamp AS mhb_last_enter_date_time, max(trim(stg.message_content)) AS message_content_text,
                                                         stg.delivered_date_time AS message_delivered_date_time,
                                                         stg.read_date_time AS message_read_date_time,
                                                         'H' AS source_system_code,
                                                         timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwgroupchatmessages AS stg
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(stg.databasename), strpos(trim(stg.databasename), '_') + 1)) = upper(trim(rdc.rdc_desc))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u10
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.recipient_username) = upper(u10.user_login_name)
   AND rdc.rdc_sid = u10.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r10
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.recipient_role), 'Unknown')) = upper(trim(r10.mhb_user_role_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = stg.patient_facility_code
   AND upper(cf2.facility_active_ind) = 'Y'
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
   AND ck.pat_acct_num = ROUND(CASE trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999'))
                                   WHEN '' THEN NUMERIC '0'
                                   ELSE CAST(trim(coalesce({{ params.param_clinical_bqutil_fns_dataset_name }}.cw_regexp_extract(stg.patient_visitnumber, '[0-9]+'), '999999999999')) AS NUMERIC)
                               END, 0, 'ROUND_HALF_EVEN')
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS u20
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(stg.sender_username) = upper(u20.user_login_name)
   AND rdc.rdc_sid = u20.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS r20
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(coalesce(trim(stg.sender_role), 'Unknown')) = upper(trim(r20.mhb_user_role_desc))
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND stg.sender_unit_id = rmu2.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf22
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf22.facility_mnemonic_cs) = trim(stg.sender_facilitycode)
   AND upper(cf22.facility_active_ind) = 'Y'
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf41
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf41.facility_mnemonic_cs) = trim(stg.patient_facility_code)
   AND upper(cf41.facility_active_ind) = 'Y'
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl5
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl5.location_mnemonic_cs) = trim(substr(stg.sender_unit_code, strpos(stg.sender_unit_code, '_') + 1))
   AND upper(cfl5.coid) = upper(cf41.coid)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu51
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu51.rdc_sid = rdc.rdc_sid
   AND stg.recipient_unit_id = rmu51.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility AS cf53
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cf53.facility_mnemonic_cs) = trim(stg.recipient_facilitycode)
   AND upper(cf53.facility_active_ind) = 'Y'
   LEFT OUTER JOIN {{ params.param_clinical_cl_base_views_dataset_name }}.clinical_facility_location AS cfl2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON trim(cfl2.location_mnemonic_cs) = trim(substr(stg.recipient_unit_code, strpos(stg.recipient_unit_code, '_') + 1))
   AND upper(cfl2.coid) = upper(cf53.coid)
   WHERE DATE(stg.sent_date_time) BETWEEN u10.eff_from_date AND u10.eff_to_date
     AND DATE(stg.sent_date_time) BETWEEN u20.eff_from_date AND u20.eff_to_date
   GROUP BY 1,
            upper(u10.user_login_name),
            3,
            4,
            5,
            upper(coalesce(ck.company_code, 'H')),
            upper(coalesce(ck.coid, '99999')),
            upper(u20.user_login_name),
            9,
            upper(coalesce(cf22.coid, '99999')),
            11,
            12,
            upper(stg.sender_platform),
            upper(stg.group_chat_name),
            upper(stg.group_chat_displayname),
            upper(CASE upper(stg.quickpick)
                      WHEN 'TRUE' THEN 'Y'
                      WHEN 'FALSE' THEN 'N'
                  END),
            upper(CASE upper(stg.urgent)
                      WHEN 'TRUE' THEN 'Y'
                      WHEN 'FALSE' THEN 'N'
                  END),
            18,
            upper(coalesce(cf41.coid, '99999')),
            20,
            21,
            22,
            upper(coalesce(cf53.coid, '99999')),
            24,
            25,
            upper(trim(stg.message_content)),
            27,
            28,
            29,
            30) AS a)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_group_chat_message.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_group_chat_message
   WHERE mhb_group_chat_message.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q)
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
