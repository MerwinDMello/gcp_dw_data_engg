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

SET srctablename = concat(srcdataset_id, '.','mon_account_payer' ); -- This needs to be added

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
FROM
        {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr
        INNER JOIN (
          SELECT
              mapcl.mon_account_payer_id,
              mapcl.schema_id,
              mapcl.payer_rank,
              max(mapcl.account_no) AS account_no,
              max(mapcl.id) AS calc_id,
              max(mapcl.calculation_date) AS calc_date,
              sum(coalesce(mapcl.total_charges, CAST(0 as NUMERIC))) AS total_charges,
              sum(coalesce(mapcl.billed_charges, CAST(0 as NUMERIC))) AS billed_charges,
              sum(coalesce(mapcl.base_expected_payment, CAST(0 as NUMERIC))) AS base_expected_payment,
              sum(coalesce(mapcl.exclusion_expected_payment, CAST(0 as NUMERIC))) AS exclusion_expected_payment,
              sum(coalesce(mapcl.length_of_service, CAST(0 as NUMERIC))) AS length_of_service,
              sum(coalesce(mapcl.total_expected_payment, CAST(0 as NUMERIC))) AS total_expected_payment
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
            WHERE mapcl.is_survivor = 1
            GROUP BY 1, 2, 3, upper(mapcl.account_no)
        ) AS clmax ON mapyr.id = clmax.mon_account_payer_id
         AND mapyr.schema_id = clmax.schema_id
        INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl ON clmax.calc_id = mpcl.id
         AND clmax.schema_id = mpcl.schema_id
        INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapyr.mon_account_id = ma.id
         AND mapyr.schema_id = ma.schema_id
        INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
         AND ma.schema_id = og.schema_id
        INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON reforg.org_id = ma.org_id_provider
         AND reforg.schema_id = ma.schema_id
        INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
         AND mapyr.schema_id = mpyr.schema_id
        INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON ma.mon_patient_type_id = mpt.id
         AND ma.schema_id = mpt.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_project AS mpj ON mapyr.mon_project_id = mpj.id
         AND mapyr.schema_id = mpj.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS macp ON coalesce(ma.mon_accounting_period_id, CAST(0 as NUMERIC)) = macp.mon_accounting_period_id
         AND ma.org_id_provider = macp.cost_report_org_id
         AND ma.schema_id = macp.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf AS mapcirf ON mpcl.id = mapcirf.mon_acct_payer_calc_summary_id
         AND mpcl.schema_id = mapcirf.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS cterm ON mpcl.cers_term_id = cterm.id
         AND mpcl.schema_id = cterm.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_irf_profile AS cirf ON mapcirf.ce_irf_profile_id = cirf.id
         AND mapcirf.schema_id = cirf.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS mapd ON ma.mon_accounting_period_id = mapd.id
         AND ma.schema_id = mapd.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.claim AS clm ON mpcl.claim_id = clm.id
         AND mpcl.schema_id = clm.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.code_versions AS cdv ON mpcl.drg_version = cdv.id
         AND mpcl.schema_id = cdv.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(mapcsvc.gvt_operating_fed_pmt, CAST(0 as NUMERIC))) AS oper_fed_pmt,
              sum(coalesce(mapcsvc.gvt_capital_cost, CAST(0 as NUMERIC))) AS gvt_cap_cost,
              --                     ,Max (Coalesce ( Mapcsvc.Cost_Weight, 0)) As Cost_Wt
              sum(coalesce(mapcsvc.gvt_outlier_payment, CAST(0 as NUMERIC))) AS gvt_out_pmt,
              sum(coalesce(mapcsvc.expected_payment, CAST(0 as NUMERIC))) AS exp_pmt,
              sum(coalesce(mapcsvc.gvt_operating_cost, CAST(0 as NUMERIC))) AS gvt_operating_cost,
              sum(coalesce(mapcsvc.ipf_pps_adj_per_diem * CASE
                WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                ELSE CAST(1 as NUMERIC)
              END, NUMERIC '0')) AS ipf_pmt,
              sum(coalesce(mapcsvc.ipf_pps_out_per_diem_add_on, CAST(0 as NUMERIC))) AS gvt_outperdiemaddon,
              sum(coalesce(mapcsvc.hac_adjmt_amt, CAST(0 as NUMERIC))) AS hac_adjmt_amt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcsvc.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 9, 10
        ) AS svc_gvt ON svc_gvt.mon_account_payer_id = clmax.mon_account_payer_id
         AND svc_gvt.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(mapcsvc2.apc_outlier_amount, CAST(0 as NUMERIC))) AS apcoutlieramt,
              sum(coalesce(mapcsvc2.expected_payment, CAST(0 as NUMERIC))) AS exp_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc2
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc2.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcsvc2.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            WHERE mapcsvc2.apc_composite_flag <> 0
             AND mapcsvc2.apc_composite_flag IS NOT NULL
            GROUP BY 3, 4
        ) AS svc_gvt2 ON svc_gvt2.mon_account_payer_id = clmax.mon_account_payer_id
         AND svc_gvt2.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              max(coalesce(mapcsvc.cost_weight, CAST(0 as NUMERIC))) AS cost_wt,
              mapcsvc.mon_acct_payer_calc_summary_id,
              mapcsvc.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcsvc.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 2, 3
        ) AS max_svc_gvt ON max_svc_gvt.mon_acct_payer_calc_summary_id = clmax.calc_id
         AND max_svc_gvt.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(mapcfs.expected_payment, CAST(0 as NUMERIC))) AS exp_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_fs AS mapcfs
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcfs.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcfs.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 2, 3
        ) AS fs ON fs.mon_account_payer_id = clmax.mon_account_payer_id
         AND fs.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(apc.expected_payment, CAST(0 as NUMERIC))) AS expected_pmt,
              sum(coalesce(apc.apc_outlier_payment, CAST(0 as NUMERIC))) AS outlier_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc AS apc
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON apc.mon_acct_payer_calc_summary_id = mapcl.id
               AND apc.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 3, 4
        ) AS calc_apc ON calc_apc.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_apc.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(apcdtl.expected_payment, CAST(0 as NUMERIC))) AS expected_pmt,
              sum(coalesce(apcdtl.apc_outlier_payment, CAST(0 as NUMERIC))) AS outlier_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_apc_comp_dtl AS apcdtl
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON apcdtl.mon_acct_payer_calc_summary_id = mapcl.id
               AND apcdtl.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 3, 4
        ) AS calc_apcdtl ON calc_apcdtl.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_apcdtl.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(triapc.expected_payment, CAST(0 as NUMERIC))) AS expected_pmt,
              sum(coalesce(triapc.tricare_apc_outlier_payment, CAST(0 as NUMERIC))) AS outlier_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_apc AS triapc
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON triapc.mon_acct_payer_calc_summary_id = mapcl.id
               AND triapc.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 3, 4
        ) AS calc_triapc ON calc_triapc.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_triapc.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(tricomp.expected_payment, CAST(0 as NUMERIC))) AS expected_pmt,
              sum(coalesce(tricomp.tricare_apc_outlier_payment, CAST(0 as NUMERIC))) AS outlier_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tricomp AS tricomp
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON tricomp.mon_acct_payer_calc_summary_id = mapcl.id
               AND tricomp.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 3, 4
        ) AS calc_tricomp ON calc_tricomp.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_tricomp.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(tridrg.drg_payment, CAST(0 as NUMERIC))) AS drg_pmt,
              sum(coalesce(tridrg.standard_cost_outlier_pay, CAST(0 as NUMERIC))) AS standard_cst_outlier_pay,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_drg AS tridrg
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON tridrg.mon_acct_payer_summary_id = mapcl.id
               AND tridrg.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 3, 4
        ) AS calc_tridrg ON calc_tridrg.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_tridrg.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(irf.outlier_payment, CAST(0 as NUMERIC))) AS irf_outlierpmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id,
              CASE
                WHEN sum(coalesce(irf.wage_rural_adj_rate, NUMERIC '0')) <> 0
                 AND sum(coalesce(irf.wage_rural_adj_rate, NUMERIC '0')) IS NOT NULL THEN sum(coalesce(irf.wage_rural_adj_rate, NUMERIC '0'))
                ELSE sum(coalesce(irf.wage_adj_rate, NUMERIC '0'))
              END AS wage_adj_rate
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf AS irf
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON irf.mon_acct_payer_calc_summary_id = mapcl.id
               AND irf.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
            GROUP BY 2, 3
        ) AS calc_irf ON calc_irf.mon_account_payer_id = clmax.mon_account_payer_id
         AND calc_irf.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(mapcsvc.expected_payment, CAST(0 as NUMERIC))) AS expected_pmt,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcsvc.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ce ON mapcsvc.ce_service_id = ce.id
               AND mapcsvc.schema_id = ce.schema_id
            WHERE upper(ce.name) LIKE 'MRI%'
             OR upper(ce.name) LIKE 'CT SCAN%'
             OR upper(ce.name) LIKE 'AMB%'
             OR upper(ce.name) LIKE 'HCD%'
             OR upper(ce.name) LIKE 'IMPLANTS%'
            GROUP BY 2, 3
        ) AS cename ON cename.mon_account_payer_id = clmax.mon_account_payer_id
         AND cename.schema_id = clmax.schema_id
        LEFT OUTER JOIN (
          SELECT
              sum(coalesce(mapcecl.amount, CAST(0 as NUMERIC))) AS amount,
              mapcl.mon_account_payer_id,
              mapcl.schema_id
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
               AND mapcecl.schema_id = mapcl.schema_id
               AND mapcl.is_survivor = 1
              INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_service_id = cexl.id
               AND mapcecl.schema_id = cexl.schema_id
            WHERE upper(cexl.description) LIKE 'MRI%'
             OR upper(cexl.description) LIKE 'CT SCAN%'
             OR upper(cexl.description) LIKE 'AMB%'
             OR upper(cexl.description) LIKE 'HCD%'
             OR upper(cexl.description) LIKE 'IMPLANTS%'
            GROUP BY 2, 3
        ) AS cexcl ON cexcl.mon_account_payer_id = clmax.mon_account_payer_id
         AND cexcl.schema_id = clmax.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS calc_fap ON clmax.calc_date BETWEEN CAST(calc_fap.start_date as DATETIME) AND CAST(calc_fap.end_date as DATETIME)
         AND clmax.schema_id = calc_fap.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS calcfirstfive_fap ON datetime_add(clmax.calc_date, interval -1 MONTH) BETWEEN CAST(calcfirstfive_fap.start_date as DATETIME) AND CAST(calcfirstfive_fap.end_date as DATETIME)
         AND clmax.schema_id = calcfirstfive_fap.schema_id
        LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS macp_13 ON CASE
          WHEN upper(rtrim(ma.billing_status)) = 'B' THEN CASE
            WHEN extract(DAY from clmax.calc_date) BETWEEN 1 AND 5 THEN CASE
              WHEN mapd.id < calc_fap.id THEN calcfirstfive_fap.id
              WHEN mapd.id >= calc_fap.id THEN mapd.id
            END
            ELSE CASE
              WHEN clmax.calc_date BETWEEN CAST(mapd.start_date as DATETIME) AND CAST(mapd.end_date as DATETIME) THEN mapd.id
              WHEN clmax.calc_date BETWEEN CAST(date_add(mapd.end_date, interval 1 DAY) as DATETIME) AND CAST(calc_fap.close_date as DATETIME) + INTERVAL 12 HOUR THEN CASE
                WHEN mapd.id <= calc_fap.id THEN calc_fap.id
                WHEN mapd.id > calc_fap.id THEN mapd.id
              END
            END
          END
        END = macp_13.mon_accounting_period_id
         AND ma.org_id_provider = macp_13.cost_report_org_id
         AND ma.schema_id = macp_13.schema_id
        LEFT OUTER JOIN (
          SELECT
              racp.schema_id,
              racp.mon_account_payer_id,
              1 AS remit_received_flag,
              sum(CASE
                WHEN ca.ra_category_id = 510 THEN ca.amount
                ELSE CAST(0 as NUMERIC)
              END) AS ra_coinsurance,
              sum(CASE
                WHEN ca.ra_category_id = 550 THEN ca.amount
                ELSE CAST(0 as NUMERIC)
              END) AS ra_deductible,
              sum(CASE
                WHEN ca.ra_category_id = 520 THEN ca.amount
                ELSE CAST(0 as NUMERIC)
              END) AS ra_copay,
              sum(CASE
                WHEN ca.ra_category_id = 570 THEN ca.amount
                ELSE CAST(0 as NUMERIC)
              END) AS ra_pt_resp_non_covered_amt
            FROM
              {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
              LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated AS ca ON ca.ra_claim_payment_id = racp.id
               AND ca.schema_id = racp.schema_id
            WHERE racp.is_deleted <> 1
            GROUP BY 1, 2
        ) AS rmt ON rmt.mon_account_payer_id = mapyr.id
         AND rmt.schema_id = mapyr.schema_id
        INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
         AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
         AND pyro.iplan_id = CASE
          WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
          ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) as INT64)
        END
        INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
         AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
         AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) as FLOAT64)
        CROSS JOIN UNNEST(ARRAY[
          concat('INS', trim(format('%11d', CAST(clmax.payer_rank as INT64))))
        ]) AS log_id
        CROSS JOIN UNNEST(ARRAY[
          reforg.coid
        ]) AS coido
        CROSS JOIN UNNEST(ARRAY[
          reforg.company_code
        ]) AS company_cd
      WHERE mpcl.service_date_begin < current_date('US/Central')
  ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor
        WHERE cc_eor.dw_last_update_date_time >=
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