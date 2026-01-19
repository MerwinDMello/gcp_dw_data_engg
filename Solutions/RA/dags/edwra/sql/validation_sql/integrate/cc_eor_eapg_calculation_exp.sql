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

SET srctablename = concat(srcdataset_id, '.','apg_calc_output' ); -- This needs to be added

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
select count(*) as row_count
FROM {{ params.param_parallon_ra_stage_dataset_name }}.apg_calc_output AS apgco
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcl.id = apgco.mon_acct_payer_calc_summary_id
   AND mapcl.schema_id = apgco.schema_id
   AND mapcl.payer_rank = 1
   AND mapcl.is_survivor = 1
   AND mapcl.is_deleted = 0
   INNER JOIN -- Filter on primary.
 {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv ON mapcl.icd_vrsn_id = pv.id
   AND mapcl.schema_id = pv.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mpyr.id = mapcl.mon_payer_id
   AND mpyr.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.org_id = mapcl.org_id
   AND rccos.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(rccos.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS cak ON upper(rtrim(cak.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(cak.company_code)) = upper(rtrim(rccos.company_code))
   AND cak.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS FLOAT64)) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}. cc_eor_eapg_calculation
        WHERE cc_eor_eapg_calculation.dw_last_update_date_time >=
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
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END