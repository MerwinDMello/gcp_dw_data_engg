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
  (SELECT f.service_guid,
          f.provider_serv_id_line_num,
          coalesce(rps.remittance_provider_serv_sid, CAST(99999 AS NUMERIC)) AS remittance_provider_serv_sid,
          'E' AS source_system_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT remittance_service.service_guid,
             remittance_service.claim_guid,
             1 AS provider_serv_id_line_num,
             remittance_service.ref_idn_qualifier1 AS provider_serv_id_qlfr_code,
             remittance_service.provider_identifier1 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier1, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier1, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       2 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier2 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier2 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier2, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier2, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       3 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier3 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier3 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier3, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier3, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       4 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier4 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier4 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier4, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier4, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       5 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier5 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier5 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier5, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier5, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       6 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier6 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier6 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier6, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier6, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       7 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier7 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier7 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier7, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier7, '')) NOT IN(''))
      UNION ALL SELECT remittance_service.service_guid,
                       remittance_service.claim_guid,
                       8 AS provider_serv_id_line_num,
                       remittance_service.ref_idn_qualifier8 AS provider_serv_id_qlfr_code,
                       remittance_service.provider_identifier8 AS provider_serv_id
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service
      WHERE upper(remittance_service.delete_ind) = 'N'
        AND (upper(coalesce(remittance_service.ref_idn_qualifier8, '')) NOT IN('')
             OR upper(coalesce(remittance_service.provider_identifier8, '')) NOT IN('')) ) AS f
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.ref_provider_service AS rps
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(rps.provider_serv_id_qlfr_code) = upper(f.provider_serv_id_qlfr_code)
   AND upper(rps.provider_serv_id) = upper(f.provider_serv_id)) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.junc_remittance_provider_serv ;)
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
