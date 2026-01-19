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
          max(mu.user_login_name) AS user_login_name,
          ck.patient_dw_id,
          CAST(trim(attachment.attach_datetime) AS DATETIME) AS attach_date_time,
          max(coalesce(mu2.user_login_name, 'Unknown')) AS creator_login_name,
          coalesce(mur2.mhb_user_role_sid, -1) AS creator_mhb_user_role_sid,
          rmu2.mhb_unit_id AS created_mhb_unit_id,
          max(coalesce(cfl2.location_mnemonic_cs, 'Unknown')) AS created_location_mnemonic_cs,
          max(coalesce(cfl2.company_code, 'H')) AS creator_company_code,
          max(coalesce(cfl2.coid, '99999')) AS creator_coid,
          max(coalesce(cf2.company_code, 'H')) AS patient_company_code,
          max(coalesce(cf2.coid, '99999')) AS patient_coid,
          ck.pat_acct_num,
          coalesce(rmur.mhb_user_role_sid, -1) AS mhb_user_role_sid,
          rmu1.mhb_unit_id AS mhb_user_signin_unit_id,
          max(coalesce(cfl1.location_mnemonic_cs, 'Unknown')) AS signin_location_mnemonic_cs,
          max(coalesce(cfl1.company_code, 'H')) AS company_code,
          max(coalesce(cfl1.coid, '99999')) AS coid,
          attachment.trail_id AS mhb_audit_trail_num,
          max(attachment.attach_source) AS detach_source_text,
          'H' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_clinical_ci_stage_dataset_name }}.vwdynamiccareattachment AS attachment
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_regional_data_center AS rdc
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(substr(trim(attachment.databasename), strpos(trim(attachment.databasename), '_') + 1)) = upper(rdc.rdc_desc)
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS mu
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(attachment.owner_user_name) = upper(mu.user_login_name)
   AND rdc.rdc_sid = mu.rdc_sid
   INNER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility AS cf2 ON trim(cf2.facility_mnemonic_cs) = trim(attachment.patient_facilitycode)
   INNER JOIN `hca-hin-dev-cur-clinical`.edwcdm_base_views.clinical_acctkeys AS ck ON upper(ck.coid) = upper(cf2.coid)
   AND ck.pat_acct_num = ROUND(CASE translate(attachment.patient_visitnumber, translate(attachment.patient_visitnumber, '0123456789', ''), '')
                                   WHEN '' THEN NUMERIC '0'
                                   ELSE CAST(translate(attachment.patient_visitnumber, translate(attachment.patient_visitnumber, '0123456789', ''), '') AS NUMERIC)
                               END, 0, 'ROUND_HALF_EVEN')
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu2.rdc_sid = rdc.rdc_sid
   AND attachment.created_unit_id = rmu2.mhb_unit_id
   INNER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_unit AS rmu1
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON rmu1.rdc_sid = rdc.rdc_sid
   AND attachment.created_unit_id = rmu1.mhb_unit_id
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS rmur
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(attachment.owner_user_role)) = upper(trim(rmur.mhb_user_role_desc))
   AND rmur.mhb_user_role_sid = mu.mhb_user_role_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.mhb_user AS mu2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(attachment.created_user_name) = upper(mu2.user_login_name)
   AND rdc.rdc_sid = mu2.rdc_sid
   LEFT OUTER JOIN {{ params.param_clinical_ci_base_views_dataset_name }}.ref_mhb_user_role AS mur2
   FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central') ON upper(trim(attachment.created_user_role)) = upper(trim(mur2.mhb_user_role_desc))
   AND mur2.mhb_user_role_sid = mu2.mhb_user_role_sid
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility_location AS cfl2 ON trim(cfl2.location_mnemonic_cs) = trim(substr(attachment.created_unitcode, strpos(attachment.created_unitcode, '_') + 1))
   AND trim(cfl2.facility_mnemonic_cs) = trim(attachment.created_facilitycode)
   LEFT OUTER JOIN `hca-hin-dev-cur-clinical`.edw_pub_views.clinical_facility_location AS cfl1 ON trim(cfl1.location_mnemonic_cs) = trim(substr(attachment.owner_unitcode, strpos(attachment.owner_unitcode, '_') + 1))
   AND trim(cfl1.facility_mnemonic_cs) = trim(attachment.owner_facilitycode)
   WHERE CAST(trim(attachment.attach_datetime) AS DATETIME) BETWEEN CAST(mu.eff_from_date AS DATETIME) AND CAST(mu.eff_to_date AS DATETIME)
     AND CAST(trim(attachment.attach_datetime) AS DATETIME) BETWEEN CAST(mu2.eff_from_date AS DATETIME) AND CAST(mu2.eff_to_date AS DATETIME)
   GROUP BY 1,
            upper(mu.user_login_name),
            3,
            4,
            upper(coalesce(mu2.user_login_name, 'Unknown')),
            6,
            7,
            upper(coalesce(cfl2.location_mnemonic_cs, 'Unknown')),
            upper(coalesce(cfl2.company_code, 'H')),
            upper(coalesce(cfl2.coid, '99999')),
            upper(coalesce(cf2.company_code, 'H')),
            upper(coalesce(cf2.coid, '99999')),
            13,
            14,
            15,
            upper(coalesce(cfl1.location_mnemonic_cs, 'Unknown')),
            upper(coalesce(cfl1.company_code, 'H')),
            upper(coalesce(cfl1.coid, '99999')),
            19,
            upper(attachment.attach_source),
            21,
            22) AS q)
  );

  SET act_values_list =
  (
  SELECT SPLIT(SOURCE_STRING,',') values_list
  FROM (SELECT format('%20d', count(*)) AS source_string
FROM
  (SELECT mhb_care_attachment.*
   FROM {{ params.param_clinical_ci_core_dataset_name }}.mhb_care_attachment
   WHERE mhb_care_attachment.dw_last_update_date_time >= tableload_start_time - INTERVAL 1 MINUTE ) AS q)
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
