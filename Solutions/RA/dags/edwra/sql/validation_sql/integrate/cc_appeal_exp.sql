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

SET srctablename = concat(srcdataset_id, '.','mon_appeal' ); -- This needs to be added

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
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS mapl
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS maps ON mapl.id = maps.mon_appeal_id
   AND mapl.schema_id = maps.schema_id
   AND maps.sequence_no = 1
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapl.mon_account_id = ma.id
   AND mapl.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_id = mapyr.mon_account_id
   AND mapl.schema_id = mapyr.schema_id
   AND mapl.mon_payer_id = mapyr.mon_payer_id
   AND mapl.payer_rank = mapyr.payer_rank
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
   AND mapyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcl.length_of_stay, CAST(0 AS NUMERIC))) AS lenofstay,
             mapcl.payer_rank AS payer_rank,
             mapcl.mon_account_id,
             mapcl.schema_id,
             mapcl.mon_account_payer_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      WHERE mapcl.is_survivor = 1
        AND mapcl.is_deleted = 0
      GROUP BY 2,
               3,
               4,
               5) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT min(maplc.lifecycle_date) AS lifecycledate,
             maplc.mon_account_payer_id,
             maplc.schema_id,
             maplc.lifecycle_event
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_lifecycle AS maplc
      WHERE maplc.lifecycle_event = 2666583
      GROUP BY 2,
               3,
               4) AS mplc ON clmax.mon_account_payer_id = mplc.mon_account_payer_id
   AND clmax.schema_id = mplc.schema_id
   LEFT OUTER JOIN
     (SELECT sum(rcp.actual_covered_days) AS actcovdays,
             rcp.mon_account_id,
             rcp.schema_id,
             rcp.mon_account_payer_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS rcp
      WHERE rcp.is_deleted = 0
      GROUP BY 2,
               3,
               4) AS racp ON racp.mon_account_id = clmax.mon_account_id
   AND racp.mon_account_payer_id = clmax.mon_account_payer_id
   AND racp.schema_id = clmax.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mapl.schema_id = usr.schema_id
   AND mapl.user_id_created_by = usr.user_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON mapl.schema_id = usr2.schema_id
   AND mapl.user_id_modified_by = usr2.user_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(po.company_code)) = upper(rtrim(rccos.company_code))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64)
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(mpyr.code)) NOT IN('NO INS',
           
                                        '000-00') ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal
      WHERE cc_appeal.dw_last_update_date_time >=
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