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
FROM (SELECT format('%20d', count(*) + 1) AS source_string
FROM
  (SELECT
     (SELECT coalesce(max(ref_remittance_corrected_priority_payor.corrected_priority_payor_sid), 0)
      FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_corrected_priority_payor
      FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) + row_number() OVER (
                                                                                                ORDER BY upper(f.corrected_priority_payor_qualifier_code),
                                                                                                         upper(f.corrected_priority_payor_id),
                                                                                                         upper(f.corrected_priority_payor_name)) AS corrected_priority_payor_sid, f.corrected_priority_payor_qualifier_code,
                                                                                                                                                                                  f.corrected_priority_payor_id,
                                                                                                                                                                                  f.corrected_priority_payor_name,
                                                                                                                                                                                  'E' AS source_system_code,
                                                                                                                                                                                  timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
   FROM
     (SELECT max(remittance_claim.corretd_prior_payor_qulifr_cod) AS corrected_priority_payor_qualifier_code,
             max(remittance_claim.corrected_priority_payor_num) AS corrected_priority_payor_id,
             max(remittance_claim.nm103_corretd_priority_payr_nm) AS corrected_priority_payor_name
      FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim
      WHERE DATE(remittance_claim.dw_last_update_date_time) =
          (SELECT max(DATE(remittance_claim_0.dw_last_update_date_time))
           FROM {{ params.param_pbs_stage_dataset_name }}.remittance_claim AS remittance_claim_0)
        AND upper(coalesce(remittance_claim.corretd_prior_payor_qulifr_cod, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.corrected_priority_payor_num, '')) NOT IN('')
        OR upper(coalesce(remittance_claim.nm103_corretd_priority_payr_nm, '')) NOT IN('')
      GROUP BY upper(remittance_claim.corretd_prior_payor_qulifr_cod),
               upper(remittance_claim.corrected_priority_payor_num),
               upper(remittance_claim.nm103_corretd_priority_payr_nm)) AS f
   WHERE (upper(f.corrected_priority_payor_qualifier_code),
          upper(f.corrected_priority_payor_id),
          upper(f.corrected_priority_payor_name)) NOT IN
       (SELECT AS STRUCT upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_qualifier_code) AS corrected_priority_payor_qualifier_code,
                         upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_id) AS corrected_priority_payor_id,
                         upper(ref_remittance_corrected_priority_payor.corrected_priority_payor_name) AS corrected_priority_payor_name
        FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_corrected_priority_payor
        FOR system_time AS OF timestamp(tableload_start_time, 'US/Central')) ) AS a ;)
);

SET act_values_list =
(
SELECT SPLIT(SOURCE_STRING,',') values_list
FROM (SELECT format('%20d', count(*)) AS source_string
FROM {{ params.param_pbs_core_dataset_name }}.ref_remittance_corrected_priority_payor
WHERE ref_remittance_corrected_priority_payor.dw_last_update_date_time >= tableload_start_time - interval 1 MINUTE ;)
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
