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

SET srctablename = concat(srcdataset_id, '.','cc_eom' ); -- This needs to be added

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
FROM (SELECT keys.patient_dw_id,
          eom.rptg_period,
          keys.company_code,
          eom.coid,
          eom.unit_num,
          eom.pat_acct_num,
          eom.admission_date,
          eom.discharge_date,
          eom.final_bill_date,
          eom.ar_bill_thru_date,
          clmax.calc_date AS eor_log_date,
          NULL AS log84_ind,
          eom.billing_status_code,
          eom.financial_class_code,
          eom.patient_type_code,
          eom.account_status_code,
          eom.total_account_balance_amt,
          eom.total_billed_charges_amt,
          eom.total_payment_amt,
          eom.total_adjustment_amt,
          eom.total_contract_allow_amt,
          coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) + coalesce(mapyr.state_tax_amt * -1, CAST(0 AS NUMERIC)) + coalesce(svc_gvt.hac_adjmt_amt * -1, CAST(0 AS BIGNUMERIC)) AS eor_gross_reimbursement_amt,
          eor.eor_gross_reimbursement_amt AS prior_day_gross_reimb_amt,
          mapyr.rate_schedule_name AS rate_schedule_name,
          eom.account_id,
          eom.dw_last_update_date_time,
          eom.source_system_code
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_eom AS eom
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS keys ON upper(rtrim(eom.coid)) = upper(rtrim(keys.coid))
   AND eom.pat_acct_num = keys.pat_acct_num
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON eom.account_id = mapyr.mon_account_id
   --AND eom.schema_id = mapyr.schema_id
   AND mapyr.payer_rank = 1
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             mapcl.payer_rank,
             max(mapcl.account_no) AS account_no,
             max(mapcl.id) AS calc_id,
             max(mapcl.calculation_date) AS calc_date,
             sum(coalesce(mapcl.total_charges, CAST(0 AS NUMERIC))) AS total_charges,
             sum(coalesce(mapcl.billed_charges, CAST(0 AS NUMERIC))) AS billed_charges,
             sum(coalesce(mapcl.base_expected_payment, CAST(0 AS NUMERIC))) AS base_expected_payment,
             sum(coalesce(mapcl.exclusion_expected_payment, CAST(0 AS NUMERIC))) AS exclusion_expected_payment,
             sum(coalesce(mapcl.length_of_service, CAST(0 AS NUMERIC))) AS length_of_service,
             sum(coalesce(mapcl.total_expected_payment, CAST(0 AS NUMERIC))) AS total_expected_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      WHERE mapcl.is_survivor = 1
      GROUP BY 1,
               2,
               3,
               upper(mapcl.account_no)) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   AND mapyr.payer_rank = 1
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl ON clmax.calc_id = mpcl.id
   AND clmax.schema_id = mpcl.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc.gvt_operating_fed_pmt, CAST(0 AS NUMERIC))) AS oper_fed_pmt,
             sum(coalesce(mapcsvc.gvt_capital_cost, CAST(0 AS NUMERIC))) AS gvt_cap_cost, --                     ,Max (Coalesce ( Mapcsvc.Cost_Weight, 0)) As Cost_Wt
 sum(coalesce(mapcsvc.gvt_outlier_payment, CAST(0 AS NUMERIC))) AS gvt_out_pmt,
 sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS exp_pmt,
 sum(coalesce(mapcsvc.gvt_operating_cost, CAST(0 AS NUMERIC))) AS gvt_operating_cost,
 sum(coalesce(mapcsvc.ipf_pps_adj_per_diem * CASE
                                                 WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                                                 ELSE CAST(1 AS NUMERIC)
                                             END, NUMERIC '0')) AS ipf_pmt,
 sum(coalesce(mapcsvc.ipf_pps_out_per_diem_add_on, CAST(0 AS NUMERIC))) AS gvt_outperdiemaddon,
 sum(coalesce(mapcsvc.hac_adjmt_amt, CAST(0 AS NUMERIC))) AS hac_adjmt_amt,
 mapcl.mon_account_payer_id,
 mapcl.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 9,
               10) AS svc_gvt ON svc_gvt.mon_account_payer_id = clmax.mon_account_payer_id
   AND svc_gvt.schema_id = clmax.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_eor AS eor ON upper(rtrim(eor.coid)) = upper(rtrim(eom.coid))
   AND eor.pat_acct_num = eom.pat_acct_num
   AND eor.iplan_insurance_order_num = 1 QUALIFY row_number() OVER (PARTITION BY keys.patient_dw_id,
                                                                                 upper(eom.rptg_period)
                                                                    ORDER BY eom.pat_acct_num) = 1)
) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eom
      WHERE cc_eom.dw_last_update_date_time >=
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