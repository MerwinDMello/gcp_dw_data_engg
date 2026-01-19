##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, audit_job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = 'ra'; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_stage_dataset_name }}" , '.') as arr));

SET srctablename = 'cc_eor_amount_all';

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET audit_job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list = -- This needs to be added
(
SELECT [format('%20d', a.row_count)]
FROM
  (
    select cast(sum(expected_value) as int64) as row_count from 
        ((select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_base'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mcare'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mcaid'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mipf'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mips'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mirf'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_mops'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_partb'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_snf'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_tricip'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
        union all
        (select  expected_value
        FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control as audit_control
        where tgt_tbl_nm = 'edwra.cc_eor_amount_tricop'
        and actual_value !=0
        group by audit_time, expected_value
        order by audit_time desc
        limit 1)
)
   ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_amount
       WHERE cc_eor_amount.dw_last_update_date_time >=
    (SELECT coalesce(max(audit_control.load_end_time), date_add(timestamp_trunc(current_datetime('US/Central'), SECOND), INTERVAL -1 DAY))
	 FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
	 WHERE upper(audit_control.job_name) = upper(audit_job_name)
	   AND audit_control.load_end_time IS NOT NULL )
   ) AS a -- This needs to be added
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
    SET audit_type = CONCAT("INGEST_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  cast(expected_value as int64), actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END