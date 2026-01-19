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

SET srctablename = concat(srcdataset_id, '.','mon_appeal_sequence' ); -- This needs to be added

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
FROM (SELECT DISTINCT po.company_code AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   a.patient_dw_id,
                   po.payor_dw_id,
                   CAST(mapl.payer_rank AS INT64) AS iplan_insurance_order_num,
                   ROUND(mapl.appeal_no, 0, 'ROUND_HALF_EVEN') AS appeal_num,
                   CAST(maps.sequence_no AS INT64) AS appeal_seq_num,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64) AS iplan,
                   ROUND(maps.appeal_balance_amt_begin, 3, 'ROUND_HALF_EVEN') AS appeal_seq_begin_bal_amt,
                   ROUND(maps.appeal_balance_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_current_bal_amt,
                   ROUND(maps.appeal_balance_end_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_end_bal_amt,
                   maps.deadline_date AS appeal_seq_deadline_date,
                   CAST(maps.sequence_close_date AS DATETIME) AS appeal_seq_close_date_time,
                   maps.apl_root_cause_id AS appeal_seq_root_cause_id,
                   maps.root_cause_detail AS appeal_seq_root_cause_dtl_text,
                   maps.apl_disposition_id AS appeal_disp_code_id,
                   maps.apl_appeal_id AS appeal_code_id,
                   substr(usr.login_id, 1, 20) AS appeal_seq_owner_user_id,
                   substr(usr2.login_id, 1, 20) AS appeal_seq_create_user_id,
                   CAST(maps.date_created AS DATETIME) AS appeal_seq_create_date_time,
                   substr(usr3.login_id, 1, 20) AS appeal_seq_update_user_id,
                   CAST(maps.date_modified AS DATETIME) AS appeal_seq_update_date_time,
                   substr(usr4.login_id, 1, 20) AS appeal_disp_id_update_user_id,
                   CAST(maps.disposition_code_modified_date AS DATETIME) AS appeal_disp_id_date_time,
                   ven.code AS vendor_cd,
                   CAST(maps.reopen_date AS DATETIME) AS appeal_seq_reopen_date_time,
                   substr(maps.reopen_user, 1, 20) AS appeal_seq_reopen_user_id,
                   CAST(maps.apl_lvl AS INT64) AS appeal_level_num,
                   maps.apl_sent_dt AS appeal_sent_date,
                   maps.prior_apl_rspn_dt AS prior_appeal_response_date,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS maps
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS mapl ON maps.mon_appeal_id = mapl.id
   AND maps.schema_id = mapl.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapl.mon_payer_id = mpyr.id
   AND mapl.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapl.mon_account_id = ma.id
   AND mapl.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_id = mapyr.mon_account_id
   AND mapl.payer_rank = mapyr.payer_rank
   AND mapl.mon_payer_id = mapyr.mon_payer_id
   AND mapl.schema_id = mapyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maps.sequence_owner_id = usr.user_id
   AND maps.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON maps.user_id_created_by = usr2.user_id
   AND maps.schema_id = usr2.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr3 ON maps.user_id_modified_by = usr3.user_id
   AND maps.schema_id = usr3.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr4 ON maps.disposition_code_modified_by = usr4.user_id
   AND maps.schema_id = usr4.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.vendor AS ven ON ven.vendor_id = maps.vendor_id
   AND ven.schema_id = maps.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64)
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(mpyr.code)) NOT IN('000-00',
                                        'NO INS') )) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
      WHERE cc_appeal_sequence.dw_last_update_date_time >=
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
  INSERT INTO edwra_ac.audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END