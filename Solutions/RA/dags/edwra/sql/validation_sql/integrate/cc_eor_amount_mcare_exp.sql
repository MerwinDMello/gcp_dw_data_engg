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

SET srctablename = concat(srcdataset_id, '.','eor_amount_stg_calc_service' ); -- This needs to be added

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
select count(*) as ROW_COUNT from
 (SELECT s.company_code,
             s.coid,
             s.patient_dw_id,
             s.payor_dw_id,
             s.iplan_insurance_order_num,
             s.eor_log_date,
             s.log_id,
             s.log_sequence_num,
             s.eff_from_date,
             s.reimbursement_method_type_code,
             15 AS amount_category_code,
             s.unit_num,
             s.eor_pat_acct_num,
             s.eor_iplan_id,
             s.map_misc_amt01 AS eor_reimbursement_amt,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
             'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND s.map_misc_amt01 <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       16 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_lab_service_charges + s.calc_atp_charge_amount + o.srv_lab_service_charges, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       30 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.mapcl_base_exp_payment + s.mapcl_exclusion_exp_payment + coalesce(s.hac_adjmt_amt * -1, CAST(0 AS NUMERIC)) - s.srv_mri_hcd_imp_exp_payment - o.srv_mri_ct_amb_exp_payment - o.srv_hcd_exp_payment - o.srv_imp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       50 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_lab_exp_payment + s.calc_atp_exp_payment + o.srv_lab_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       83 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_mri_exp_payment + o.srv_mri_ct_amb_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       85 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_hcd_exp_payment + o.srv_hcd_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       86 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.map_total_exp_payment - (s.mapcl_base_exp_payment + s.mapcl_exclusion_exp_payment + s.map_sequestration_amt), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       99 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
      AND o.schema_id = s.schema_id
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_imp_exp_payment + o.srv_imp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0
      UNION ALL SELECT s.company_code,
                       s.coid,
                       s.patient_dw_id,
                       s.payor_dw_id,
                       s.iplan_insurance_order_num,
                       s.eor_log_date,
                       s.log_id,
                       s.log_sequence_num,
                       s.eff_from_date,
                       s.reimbursement_method_type_code,
                       142 AS amount_category_code,
                       s.unit_num,
                       s.eor_pat_acct_num,
                       s.eor_iplan_id,
                       eor_reimbursement_amt AS eor_reimbursement_amt,
                       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                       'N' AS source_system_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
      CROSS JOIN UNNEST(ARRAY[ ROUND(s.dsh_uncmps_care_addon_amt, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
      WHERE upper(rtrim(s.rate_schedule)) = 'MCARE'
        AND eor_reimbursement_amt <> 0 )
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
	-- WHERE upper(audit_control.job_name) = upper(audit_job_name)
  WHERE tgt_tbl_nm LIKE CONCAT(tgtdataset_id, '.cc_eor_amount_%')
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