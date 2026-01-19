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
  (SELECT max(scc.service_guid) AS service_guid,
          max(scc.adj_group_code) AS adj_group_code,
          max(scc.carc_code) AS carc_code,
          scc.audit_date,
          max(scc.delete_ind) AS delete_ind,
          scc.delete_date,
          max(svc.coid) AS coid,
          max(svc.company_code) AS company_code,
          sum(scc.adj_amt) AS adj_amt,
          sum(scc.adj_qty) AS adj_qty,
          max(scc.adj_category) AS adj_category,
          max(scc.cc_adj_group_code) AS cc_adj_group_code,
          timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time,
          'E' AS source_system_code
   FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service_carc AS scc
   LEFT OUTER JOIN {{ params.param_pbs_base_views_dataset_name }}.remittance_service AS svc
   FOR system_time AS OF timestamp(tableload_start_time, 'US/Central') ON upper(svc.service_guid) = upper(scc.service_guid)
   WHERE scc.audit_date =
       (SELECT max(remittance_service_carc.audit_date)
        FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service_carc)
   GROUP BY upper(scc.service_guid),
            upper(scc.adj_group_code),
            upper(scc.carc_code),
            4,
            upper(scc.delete_ind),
            6,
            upper(svc.coid),
            upper(svc.company_code),
            upper(scc.adj_category),
            upper(scc.cc_adj_group_code),
            13,
            14) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.remittance_service_carc
WHERE remittance_service_carc.audit_date =
    (SELECT max(remittance_service_carc_0.audit_date)
     FROM {{ params.param_pbs_stage_dataset_name }}.remittance_service_carc AS remittance_service_carc_0) ;)
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
