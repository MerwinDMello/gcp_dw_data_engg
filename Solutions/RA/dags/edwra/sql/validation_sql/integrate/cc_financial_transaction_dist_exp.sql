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

SET srctablename = concat(srcdataset_id, '.','mon_account_transaction_dist' ); -- This needs to be added

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
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_transaction_dist AS matd
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_transaction AS mat ON matd.mon_account_transaction_id = mat.id
AND matd.schema_id = mat.schema_id
INNER JOIN (SELECT ma.account_no AS account_no,
       ma.id AS id,
       ma.schema_id AS schema_id,
       ma.org_id_provider AS org_id_provider,
       og.client_id AS og_client_id,
       og.short_name AS short_name,
       og.org_id AS org_id,
       og.schema_id AS og_schema_id
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON og.org_id = ma.org_id_provider
AND og.schema_id = ma.schema_id
WHERE DATE(ma.service_date_begin) >= DATE '2011-09-01') ma ON ma.id = mat.mon_account_id
AND ma.schema_id = mat.schema_id
INNER JOIN /*JOIN ra_edwra_STAGING.Mon_Account MA ON
               MA.Id = MAT.Mon_Account_Id AND
               MA.Schema_Id = MAT.Schema_Id AND
               MA.Service_Date_Begin >= DATE '2011-09-01'
          JOIN ra_edwra_STAGING.Org OG ON
               OG.Org_Id = MA.Org_Id_Provider AND
               OG.Schema_Id = MA.Schema_Id   */ {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON usr.user_id = matd.user_id_redistributed_by
AND usr.schema_id = matd.schema_id
INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(ma.og_client_id, 7, 5)))
AND reforg.schema_id = ma.og_schema_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
AND a.pat_acct_num = ma.account_no) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction_dist
       WHERE cc_financial_transaction_dist.dw_last_update_date_time >=
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