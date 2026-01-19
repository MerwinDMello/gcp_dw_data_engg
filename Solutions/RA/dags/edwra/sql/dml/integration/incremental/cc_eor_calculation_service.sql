DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_calculation_service.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: PT
      Name: CC_EOR_Calculation_Service - BTEQ Script.
      Mod1: Creation of script on 09/29/2021.
      Mod2: Pulling a column named IPF_PPS_FACILITY_ADJUST_FACTOR, but should be pulling IPF_PPS_FACILITY_ADJ_FACTOR
***************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA401;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg AS mt USING
  (SELECT DISTINCT rccos.coid AS coid,
                   rccos.company_code AS company_code,
                   a.patient_dw_id,
                   po.payor_dw_id,
                   CAST(mapcl.payer_rank AS INT64) AS iplan_insurance_order_num,
                   rccos.unit_num AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num, -- Case When Trim (MP.Code) = 'NO INS' Then 0 Else Cast (Substr ( Trim (MP.Code), 1, 3) || Substr ( Trim (MP.Code), 5, 2) As Integer) End As IPLAN_ID,
 po.iplan_id AS iplan_id,
 DATE(mapcl.calculation_date) AS eor_log_date,
 mapcsvc.id AS acct_payer_calc_service_id,
 mapcl.id AS calc_id,
 CAST(mapcsvc.ce_service_id AS INT64) AS ce_service_id,
 mapcsvc.service_date,
 CAST(mapcsvc.quantity AS INT64) AS service_qty,
 ROUND(mapcsvc.expected_payment, 3, 'ROUND_HALF_EVEN') AS expected_payment_amt,
 mapcsvc.subterm_expected_payment AS subterm_expected_pmt_amt,
 ROUND(mapcsvc.service_charges, 3, 'ROUND_HALF_EVEN') AS service_charge_amt,
 ROUND(mapcsvc.exclusion_charges, 3, 'ROUND_HALF_EVEN') AS exclusion_charge_amt,
 ROUND(mapcsvc.noncovered_charges, 3, 'ROUND_HALF_EVEN') AS noncovered_charge_amt,
 mapcsvc.procedure_code_type AS proc_code_type,
 substr(mapcsvc.procedure_code, 1, 10) AS proc_code,
 substr(mapcsvc.modifer_1, 1, 2) AS modifier_1,
 substr(mapcsvc.modifer_2, 1, 2) AS modifier_2,
 substr(mapcsvc.revenue_code, 1, 4) AS revenue_code_desc,
 substr(pv_calc_method.display_text, 1, 200) AS calculation_method,
 CAST(mapcsvc.rate_info_from AS INT64) AS rate_info_from,
 CAST(mapcsvc.rate_info_to AS INT64) AS rate_info_to,
 ROUND(mapcsvc.rate_info_amount, 3, 'ROUND_HALF_EVEN') AS rate_info_amt,
 substr(mapcsvc.rate_info_service_display, 1, 200) AS rate_info_service_desc,
 mapcsvc.gvt_drg_pmt AS gov_drg_payment_amt,
 mapcsvc.gvt_ime_capital_drg_pmt AS gov_ime_capital_drg_pmt_amt,
 mapcsvc.gvt_operating_cost_outlier AS gov_operating_cost_outlier_amt,
 mapcsvc.gvt_operating_fed_pmt AS gov_operating_fed_pmt_amt,
 mapcsvc.hipps_payment AS hipps_pmt_amt,
 mapcsvc.ipf_pps_outlier_per_diem AS ipf_pps_outlier_per_diem_amt,
 mapcsvc.ipf_pps_out_per_diem_add_on AS ipf_pps_out_per_diem_addon_amt,
 mapcsvc.gvt_scp_payment AS gov_scp_pmt,
 mapcsvc.ipf_cost_charge_ratio AS ipf_cost_charge_ratio,
 ROUND(mapcsvc.ipf_fixed_loss_amount, 6, 'ROUND_HALF_EVEN') AS ipf_fixed_loss_amt,
 mapcsvc.ipf_labor_share_amount AS ipf_labor_share_amt,
 mapcsvc.ipf_labor_share_percent AS ipf_labor_share_pct,
 ROUND(mapcsvc.ipf_non_labor_share_amount, 6, 'ROUND_HALF_EVEN') AS ipf_non_labor_share_amt,
 ROUND(mapcsvc.ipf_teaching_adjst_factor, 6, 'ROUND_HALF_EVEN') AS ipf_teaching_adj_fctr,
 ROUND(mapcsvc.ipf_age_factor, 6, 'ROUND_HALF_EVEN') AS ipf_age_fctr,
 ROUND(mapcsvc.ipf_drg_adjust_factor, 6, 'ROUND_HALF_EVEN') AS ipf_drg_adj_fctr,
 ROUND(mapcsvc.ipf_comorbidity_factor, 6, 'ROUND_HALF_EVEN') AS ipf_comorbidity_fctr,
 ROUND(mapcsvc.ipf_pps_facility_adj_factor, 2, 'ROUND_HALF_EVEN') AS ipf_pps_facility_adj_fctr,
 mapcsvc.ipf_pps_adjustment_factor AS ipf_pps_adj_fctr,
 ROUND(mapcsvc.ipf_pps_los_adj_factor, 2, 'ROUND_HALF_EVEN') AS ipf_pps_los_adj_fctr,
 mapcsvc.gvt_newtech_add_on AS gov_newtech_add_on_amt,
 mapcsvc.gvt_device_offset_reduction AS gov_device_offset_reduction,
 mapcsvc.gvt_transfer_percent AS gov_transfer_pct,
 CAST(mapcsvc.ipf_per_diem_day AS INT64) AS ipf_per_diem_day,
 CAST(mapcsvc.apc_composite_flag AS INT64) AS apc_composite_flag,
 mapcsvc.dsh_unadj_opr_addon_amt AS dsh_unadj_opr_addon_amt,
 mapcsvc.dsh_uncmps_care_addon_amt AS dsh_uncmps_care_addon_amt,
 mapcsvc.mdh_add_on_amt AS mdh_add_on_amt,
 mapcsvc.low_vol_add_on_amt AS low_vol_add_on_amt,
 mapcsvc.readm_adjmt_amt AS readm_adj_amt,
 mapcsvc.val_based_adjmt_amt AS val_based_adj_amt,
 cdr.fixed_loss_threshold AS drg_fixed_loss_threshold,
 cdr.marginal_cost_factor AS drg_marginal_cost_fctr,
 apc.apc_outlier_pct,
 coin_fs.opps_pct,
 'N' AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = mapcl.schema_id
   AND rccos.org_id = mapcl.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mp ON mp.schema_id = mapcl.schema_id
   AND mp.id = mapcl.mon_payer_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(trim(a.coid)) = upper(trim(rccos.coid))
   AND upper(trim(a.company_code)) = upper(trim(rccos.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS FLOAT64)
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(trim(po.coid)) = upper(trim(rccos.coid))
   AND upper(trim(po.company_code)) = upper(trim(rccos.company_code))
   AND po.iplan_id = CASE
                         WHEN upper(trim(mp.code)) = 'NO INS' THEN 0
                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mp.code), 1, 3), substr(trim(mp.code), 5, 2))) AS INT64)
                     END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc ON mapcsvc.schema_id = mapcl.schema_id
   AND mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv_calc_method ON mapcsvc.schema_id = pv_calc_method.schema_id
   AND mapcsvc.calculation_method_id = pv_calc_method.id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS mapcl_ct ON mapcl_ct.schema_id = mapcl.schema_id
   AND mapcl_ct.id = mapcl.cers_term_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_drg_rate AS cdr ON cdr.schema_id = mapcl.schema_id
   AND cdr.cers_term_id = CASE
                              WHEN mapcl_ct.cers_parent_term_id IS NULL THEN mapcl.cers_term_id
                              ELSE mapcl_ct.cers_parent_term_id
                          END
   AND upper(trim(cdr.drg_type)) = upper(trim(mapcl.drg_type))
   AND upper(trim(cdr.drg)) = upper(trim(mapcl.drg))
   LEFT OUTER JOIN
     (SELECT DISTINCT apc_comp_dtl.schema_id,
                      apc_comp_dtl.mon_acct_payer_calc_summary_id,
                      apc_comp_dtl.ce_service_id,
                      apc_comp_dtl.ce_rule_id, -- APC_COMP_DTL.APC_CODE,
 apc_comp_dtl.apc_outlier_percent AS apc_outlier_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_apc_comp_dtl AS apc_comp_dtl
      UNION ALL SELECT DISTINCT mapc_apc.schema_id,
                                mapc_apc.mon_acct_payer_calc_summary_id,
                                mapc_apc.ce_service_id,
                                mapc_apc.ce_rule_id, -- MAPC_APC.APC_CODE,
 mapc_apc.apc_outlier_percent AS apc_outlier_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc AS mapc_apc) AS apc ON apc.schema_id = mapcl.schema_id
   AND apc.mon_acct_payer_calc_summary_id = mapcl.id
   AND apc.ce_service_id = mapcsvc.ce_service_id
   AND apc.ce_rule_id = mapcsvc.ce_rule_id
   LEFT OUTER JOIN
     (SELECT DISTINCT mapcsvc2.mon_acct_payer_calc_summary_id,
                      mapcsvc2.schema_id,
                      mapcsvc2.ce_service_id,
                      coin_fs2.cers_rate_amount AS opps_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl2
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc2 ON mapcsvc2.schema_id = mapcl2.schema_id
      AND mapcsvc2.mon_acct_payer_calc_summary_id = mapcl2.id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_coin_fs AS coin_fs2 ON coin_fs2.schema_id = mapcl2.schema_id
      AND coin_fs2.mon_acct_payer_calc_summary_id = mapcl2.id
      AND coin_fs2.ce_service_id = mapcsvc2.ce_service_id
      WHERE mapcsvc2.ce_rule_type_id = 40 ) AS coin_fs ON coin_fs.schema_id = mapcl.schema_id
   AND coin_fs.mon_acct_payer_calc_summary_id = mapcl.id
   AND coin_fs.ce_service_id = mapcsvc.ce_service_id) AS ms ON upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.eor_log_date, DATE '1970-01-01') = coalesce(ms.eor_log_date, DATE '1970-01-01')
     AND coalesce(mt.eor_log_date, DATE '1970-01-02') = coalesce(ms.eor_log_date, DATE '1970-01-02'))
AND (coalesce(mt.acct_payor_calc_service_id, NUMERIC '0') = coalesce(ms.acct_payer_calc_service_id, NUMERIC '0')
     AND coalesce(mt.acct_payor_calc_service_id, NUMERIC '1') = coalesce(ms.acct_payer_calc_service_id, NUMERIC '1'))
AND (coalesce(mt.calc_id, NUMERIC '0') = coalesce(ms.calc_id, NUMERIC '0')
     AND coalesce(mt.calc_id, NUMERIC '1') = coalesce(ms.calc_id, NUMERIC '1'))
AND (coalesce(mt.ce_service_id, 0) = coalesce(ms.ce_service_id, 0)
     AND coalesce(mt.ce_service_id, 1) = coalesce(ms.ce_service_id, 1))
AND (coalesce(mt.service_date, DATE '1970-01-01') = coalesce(ms.service_date, DATE '1970-01-01')
     AND coalesce(mt.service_date, DATE '1970-01-02') = coalesce(ms.service_date, DATE '1970-01-02'))
AND (coalesce(mt.service_qty, 0) = coalesce(ms.service_qty, 0)
     AND coalesce(mt.service_qty, 1) = coalesce(ms.service_qty, 1))
AND (coalesce(mt.expected_payment_amt, NUMERIC '0') = coalesce(ms.expected_payment_amt, NUMERIC '0')
     AND coalesce(mt.expected_payment_amt, NUMERIC '1') = coalesce(ms.expected_payment_amt, NUMERIC '1'))
AND (coalesce(mt.subterm_expected_payment_amt, NUMERIC '0') = coalesce(ms.subterm_expected_pmt_amt, NUMERIC '0')
     AND coalesce(mt.subterm_expected_payment_amt, NUMERIC '1') = coalesce(ms.subterm_expected_pmt_amt, NUMERIC '1'))
AND (coalesce(mt.service_charge_amt, NUMERIC '0') = coalesce(ms.service_charge_amt, NUMERIC '0')
     AND coalesce(mt.service_charge_amt, NUMERIC '1') = coalesce(ms.service_charge_amt, NUMERIC '1'))
AND (coalesce(mt.exclusion_charge_amt, NUMERIC '0') = coalesce(ms.exclusion_charge_amt, NUMERIC '0')
     AND coalesce(mt.exclusion_charge_amt, NUMERIC '1') = coalesce(ms.exclusion_charge_amt, NUMERIC '1'))
AND (coalesce(mt.noncovered_charge_amt, NUMERIC '0') = coalesce(ms.noncovered_charge_amt, NUMERIC '0')
     AND coalesce(mt.noncovered_charge_amt, NUMERIC '1') = coalesce(ms.noncovered_charge_amt, NUMERIC '1'))
AND (upper(coalesce(mt.proc_code_type_desc, '0')) = upper(coalesce(ms.proc_code_type, '0'))
     AND upper(coalesce(mt.proc_code_type_desc, '1')) = upper(coalesce(ms.proc_code_type, '1')))
AND (upper(coalesce(mt.proc_code, '0')) = upper(coalesce(ms.proc_code, '0'))
     AND upper(coalesce(mt.proc_code, '1')) = upper(coalesce(ms.proc_code, '1')))
AND (upper(coalesce(mt.modifier_1_code, '0')) = upper(coalesce(ms.modifier_1, '0'))
     AND upper(coalesce(mt.modifier_1_code, '1')) = upper(coalesce(ms.modifier_1, '1')))
AND (upper(coalesce(mt.modifier_2_code, '0')) = upper(coalesce(ms.modifier_2, '0'))
     AND upper(coalesce(mt.modifier_2_code, '1')) = upper(coalesce(ms.modifier_2, '1')))
AND (upper(coalesce(mt.revenue_code_desc, '0')) = upper(coalesce(ms.revenue_code_desc, '0'))
     AND upper(coalesce(mt.revenue_code_desc, '1')) = upper(coalesce(ms.revenue_code_desc, '1')))
AND (upper(coalesce(mt.calculation_method_desc, '0')) = upper(coalesce(ms.calculation_method, '0'))
     AND upper(coalesce(mt.calculation_method_desc, '1')) = upper(coalesce(ms.calculation_method, '1')))
AND (coalesce(mt.rate_info_from_day_num, 0) = coalesce(ms.rate_info_from, 0)
     AND coalesce(mt.rate_info_from_day_num, 1) = coalesce(ms.rate_info_from, 1))
AND (coalesce(mt.rate_info_to_day_num, 0) = coalesce(ms.rate_info_to, 0)
     AND coalesce(mt.rate_info_to_day_num, 1) = coalesce(ms.rate_info_to, 1))
AND (coalesce(mt.rate_info_amt, NUMERIC '0') = coalesce(ms.rate_info_amt, NUMERIC '0')
     AND coalesce(mt.rate_info_amt, NUMERIC '1') = coalesce(ms.rate_info_amt, NUMERIC '1'))
AND (upper(coalesce(mt.rate_info_service_desc, '0')) = upper(coalesce(ms.rate_info_service_desc, '0'))
     AND upper(coalesce(mt.rate_info_service_desc, '1')) = upper(coalesce(ms.rate_info_service_desc, '1')))
AND (coalesce(mt.gov_drg_payment_amt, NUMERIC '0') = coalesce(ms.gov_drg_payment_amt, NUMERIC '0')
     AND coalesce(mt.gov_drg_payment_amt, NUMERIC '1') = coalesce(ms.gov_drg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.gov_ime_capital_drg_pmt_amt, NUMERIC '0') = coalesce(ms.gov_ime_capital_drg_pmt_amt, NUMERIC '0')
     AND coalesce(mt.gov_ime_capital_drg_pmt_amt, NUMERIC '1') = coalesce(ms.gov_ime_capital_drg_pmt_amt, NUMERIC '1'))
AND (coalesce(mt.gov_operating_cost_outlier_amt, NUMERIC '0') = coalesce(ms.gov_operating_cost_outlier_amt, NUMERIC '0')
     AND coalesce(mt.gov_operating_cost_outlier_amt, NUMERIC '1') = coalesce(ms.gov_operating_cost_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.gov_operating_fed_pmt_amt, NUMERIC '0') = coalesce(ms.gov_operating_fed_pmt_amt, NUMERIC '0')
     AND coalesce(mt.gov_operating_fed_pmt_amt, NUMERIC '1') = coalesce(ms.gov_operating_fed_pmt_amt, NUMERIC '1'))
AND (coalesce(mt.hipps_payment_amt, NUMERIC '0') = coalesce(ms.hipps_pmt_amt, NUMERIC '0')
     AND coalesce(mt.hipps_payment_amt, NUMERIC '1') = coalesce(ms.hipps_pmt_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_outlier_per_diem_amt, NUMERIC '0') = coalesce(ms.ipf_pps_outlier_per_diem_amt, NUMERIC '0')
     AND coalesce(mt.ipf_pps_outlier_per_diem_amt, NUMERIC '1') = coalesce(ms.ipf_pps_outlier_per_diem_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_out_per_diem_addon_amt, NUMERIC '0') = coalesce(ms.ipf_pps_out_per_diem_addon_amt, NUMERIC '0')
     AND coalesce(mt.ipf_pps_out_per_diem_addon_amt, NUMERIC '1') = coalesce(ms.ipf_pps_out_per_diem_addon_amt, NUMERIC '1'))
AND (coalesce(mt.gov_scp_payment_amt, NUMERIC '0') = coalesce(ms.gov_scp_pmt, NUMERIC '0')
     AND coalesce(mt.gov_scp_payment_amt, NUMERIC '1') = coalesce(ms.gov_scp_pmt, NUMERIC '1'))
AND (coalesce(mt.ipf_cost_charge_ratio_amt, NUMERIC '0') = coalesce(ms.ipf_cost_charge_ratio, NUMERIC '0')
     AND coalesce(mt.ipf_cost_charge_ratio_amt, NUMERIC '1') = coalesce(ms.ipf_cost_charge_ratio, NUMERIC '1'))
AND (coalesce(mt.ipf_fixed_loss_amt, NUMERIC '0') = coalesce(ms.ipf_fixed_loss_amt, NUMERIC '0')
     AND coalesce(mt.ipf_fixed_loss_amt, NUMERIC '1') = coalesce(ms.ipf_fixed_loss_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_labor_share_amt, NUMERIC '0') = coalesce(ms.ipf_labor_share_amt, NUMERIC '0')
     AND coalesce(mt.ipf_labor_share_amt, NUMERIC '1') = coalesce(ms.ipf_labor_share_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_labor_share_pct, NUMERIC '0') = coalesce(ms.ipf_labor_share_pct, NUMERIC '0')
     AND coalesce(mt.ipf_labor_share_pct, NUMERIC '1') = coalesce(ms.ipf_labor_share_pct, NUMERIC '1'))
AND (coalesce(mt.ipf_non_labor_share_amt, NUMERIC '0') = coalesce(ms.ipf_non_labor_share_amt, NUMERIC '0')
     AND coalesce(mt.ipf_non_labor_share_amt, NUMERIC '1') = coalesce(ms.ipf_non_labor_share_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_teaching_adj_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_teaching_adj_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_teaching_adj_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_teaching_adj_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_age_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_age_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_age_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_age_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_drg_adj_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_drg_adj_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_drg_adj_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_drg_adj_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_comorbidity_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_comorbidity_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_comorbidity_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_comorbidity_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_facility_adj_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_pps_facility_adj_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_pps_facility_adj_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_pps_facility_adj_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_adj_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_pps_adj_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_pps_adj_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_pps_adj_fctr, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_los_adj_fctr_pct, NUMERIC '0') = coalesce(ms.ipf_pps_los_adj_fctr, NUMERIC '0')
     AND coalesce(mt.ipf_pps_los_adj_fctr_pct, NUMERIC '1') = coalesce(ms.ipf_pps_los_adj_fctr, NUMERIC '1'))
AND (coalesce(mt.gov_new_tech_add_on_amt, NUMERIC '0') = coalesce(ms.gov_newtech_add_on_amt, NUMERIC '0')
     AND coalesce(mt.gov_new_tech_add_on_amt, NUMERIC '1') = coalesce(ms.gov_newtech_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.gov_device_offset_red_amt, NUMERIC '0') = coalesce(ms.gov_device_offset_reduction, NUMERIC '0')
     AND coalesce(mt.gov_device_offset_red_amt, NUMERIC '1') = coalesce(ms.gov_device_offset_reduction, NUMERIC '1'))
AND (coalesce(mt.gov_transfer_pct, NUMERIC '0') = coalesce(ms.gov_transfer_pct, NUMERIC '0')
     AND coalesce(mt.gov_transfer_pct, NUMERIC '1') = coalesce(ms.gov_transfer_pct, NUMERIC '1'))
AND (coalesce(mt.ipf_per_diem_day_num, 0) = coalesce(ms.ipf_per_diem_day, 0)
     AND coalesce(mt.ipf_per_diem_day_num, 1) = coalesce(ms.ipf_per_diem_day, 1))
AND (coalesce(mt.apc_composite_flg, 0) = coalesce(ms.apc_composite_flag, 0)
     AND coalesce(mt.apc_composite_flg, 1) = coalesce(ms.apc_composite_flag, 1))
AND (coalesce(mt.dsh_unadj_opr_add_on_amt, NUMERIC '0') = coalesce(ms.dsh_unadj_opr_addon_amt, NUMERIC '0')
     AND coalesce(mt.dsh_unadj_opr_add_on_amt, NUMERIC '1') = coalesce(ms.dsh_unadj_opr_addon_amt, NUMERIC '1'))
AND (coalesce(mt.dsh_uncmps_care_add_on_amt, NUMERIC '0') = coalesce(ms.dsh_uncmps_care_addon_amt, NUMERIC '0')
     AND coalesce(mt.dsh_uncmps_care_add_on_amt, NUMERIC '1') = coalesce(ms.dsh_uncmps_care_addon_amt, NUMERIC '1'))
AND (coalesce(mt.mdh_add_on_amt, NUMERIC '0') = coalesce(ms.mdh_add_on_amt, NUMERIC '0')
     AND coalesce(mt.mdh_add_on_amt, NUMERIC '1') = coalesce(ms.mdh_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.low_vol_add_on_amt, NUMERIC '0') = coalesce(ms.low_vol_add_on_amt, NUMERIC '0')
     AND coalesce(mt.low_vol_add_on_amt, NUMERIC '1') = coalesce(ms.low_vol_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.readm_adj_amt, NUMERIC '0') = coalesce(ms.readm_adj_amt, NUMERIC '0')
     AND coalesce(mt.readm_adj_amt, NUMERIC '1') = coalesce(ms.readm_adj_amt, NUMERIC '1'))
AND (coalesce(mt.val_based_adj_amt, NUMERIC '0') = coalesce(ms.val_based_adj_amt, NUMERIC '0')
     AND coalesce(mt.val_based_adj_amt, NUMERIC '1') = coalesce(ms.val_based_adj_amt, NUMERIC '1'))
AND (coalesce(mt.drg_fixed_loss_threshold_amt, NUMERIC '0') = coalesce(ms.drg_fixed_loss_threshold, NUMERIC '0')
     AND coalesce(mt.drg_fixed_loss_threshold_amt, NUMERIC '1') = coalesce(ms.drg_fixed_loss_threshold, NUMERIC '1'))
AND (coalesce(mt.drg_marginal_cost_fctr_pct, NUMERIC '0') = coalesce(ms.drg_marginal_cost_fctr, NUMERIC '0')
     AND coalesce(mt.drg_marginal_cost_fctr_pct, NUMERIC '1') = coalesce(ms.drg_marginal_cost_fctr, NUMERIC '1'))
AND (coalesce(mt.apc_outlier_pct, NUMERIC '0') = coalesce(ms.apc_outlier_pct, NUMERIC '0')
     AND coalesce(mt.apc_outlier_pct, NUMERIC '1') = coalesce(ms.apc_outlier_pct, NUMERIC '1'))
AND (coalesce(mt.opps_pct, NUMERIC '0') = coalesce(ms.opps_pct, NUMERIC '0')
     AND coalesce(mt.opps_pct, NUMERIC '1') = coalesce(ms.opps_pct, NUMERIC '1'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (coid,
        company_code,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        eor_log_date,
        acct_payor_calc_service_id,
        calc_id,
        ce_service_id,
        service_date,
        service_qty,
        expected_payment_amt,
        subterm_expected_payment_amt,
        service_charge_amt,
        exclusion_charge_amt,
        noncovered_charge_amt,
        proc_code_type_desc,
        proc_code,
        modifier_1_code,
        modifier_2_code,
        revenue_code_desc,
        calculation_method_desc,
        rate_info_from_day_num,
        rate_info_to_day_num,
        rate_info_amt,
        rate_info_service_desc,
        gov_drg_payment_amt,
        gov_ime_capital_drg_pmt_amt,
        gov_operating_cost_outlier_amt,
        gov_operating_fed_pmt_amt,
        hipps_payment_amt,
        ipf_pps_outlier_per_diem_amt,
        ipf_pps_out_per_diem_addon_amt,
        gov_scp_payment_amt,
        ipf_cost_charge_ratio_amt,
        ipf_fixed_loss_amt,
        ipf_labor_share_amt,
        ipf_labor_share_pct,
        ipf_non_labor_share_amt,
        ipf_teaching_adj_fctr_pct,
        ipf_age_fctr_pct,
        ipf_drg_adj_fctr_pct,
        ipf_comorbidity_fctr_pct,
        ipf_pps_facility_adj_fctr_pct,
        ipf_pps_adj_fctr_pct,
        ipf_pps_los_adj_fctr_pct,
        gov_new_tech_add_on_amt,
        gov_device_offset_red_amt,
        gov_transfer_pct,
        ipf_per_diem_day_num,
        apc_composite_flg,
        dsh_unadj_opr_add_on_amt,
        dsh_uncmps_care_add_on_amt,
        mdh_add_on_amt,
        low_vol_add_on_amt,
        readm_adj_amt,
        val_based_adj_amt,
        drg_fixed_loss_threshold_amt,
        drg_marginal_cost_fctr_pct,
        apc_outlier_pct,
        opps_pct,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.coid, ms.company_code, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.eor_log_date, ms.acct_payer_calc_service_id, ms.calc_id, ms.ce_service_id, ms.service_date, ms.service_qty, ms.expected_payment_amt, ms.subterm_expected_pmt_amt, ms.service_charge_amt, ms.exclusion_charge_amt, ms.noncovered_charge_amt, ms.proc_code_type, ms.proc_code, ms.modifier_1, ms.modifier_2, ms.revenue_code_desc, ms.calculation_method, ms.rate_info_from, ms.rate_info_to, ms.rate_info_amt, ms.rate_info_service_desc, ms.gov_drg_payment_amt, ms.gov_ime_capital_drg_pmt_amt, ms.gov_operating_cost_outlier_amt, ms.gov_operating_fed_pmt_amt, ms.hipps_pmt_amt, ms.ipf_pps_outlier_per_diem_amt, ms.ipf_pps_out_per_diem_addon_amt, ms.gov_scp_pmt, ms.ipf_cost_charge_ratio, ms.ipf_fixed_loss_amt, ms.ipf_labor_share_amt, ms.ipf_labor_share_pct, ms.ipf_non_labor_share_amt, ms.ipf_teaching_adj_fctr, ms.ipf_age_fctr, ms.ipf_drg_adj_fctr, ms.ipf_comorbidity_fctr, ms.ipf_pps_facility_adj_fctr, ms.ipf_pps_adj_fctr, ms.ipf_pps_los_adj_fctr, ms.gov_newtech_add_on_amt, ms.gov_device_offset_reduction, ms.gov_transfer_pct, ms.ipf_per_diem_day, ms.apc_composite_flag, ms.dsh_unadj_opr_addon_amt, ms.dsh_uncmps_care_addon_amt, ms.mdh_add_on_amt, ms.low_vol_add_on_amt, ms.readm_adj_amt, ms.val_based_adj_amt, ms.drg_fixed_loss_threshold, ms.drg_marginal_cost_fctr, ms.apc_outlier_pct, ms.opps_pct, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             acct_payor_calc_service_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               acct_payor_calc_service_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_EOR_Calculation_Service_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) 
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = mapcl.schema_id
   AND rccos.org_id = mapcl.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mp ON mp.schema_id = mapcl.schema_id
   AND mp.id = mapcl.mon_payer_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(trim(a.coid)) = upper(trim(rccos.coid))
   AND upper(trim(a.company_code)) = upper(trim(rccos.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS FLOAT64)
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(trim(po.coid)) = upper(trim(rccos.coid))
   AND upper(trim(po.company_code)) = upper(trim(rccos.company_code))
   AND po.iplan_id = CASE
                         WHEN upper(trim(mp.code)) = 'NO INS' THEN 0
                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mp.code), 1, 3), substr(trim(mp.code), 5, 2))) AS INT64)
                     END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc ON mapcsvc.schema_id = mapcl.schema_id
   AND mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv_calc_method ON mapcsvc.schema_id = pv_calc_method.schema_id
   AND mapcsvc.calculation_method_id = pv_calc_method.id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS mapcl_ct ON mapcl_ct.schema_id = mapcl.schema_id
   AND mapcl_ct.id = mapcl.cers_term_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_drg_rate AS cdr ON cdr.schema_id = mapcl.schema_id
   AND cdr.cers_term_id = CASE
                              WHEN mapcl_ct.cers_parent_term_id IS NULL THEN mapcl.cers_term_id
                              ELSE mapcl_ct.cers_parent_term_id
                          END
   AND upper(trim(cdr.drg_type)) = upper(trim(mapcl.drg_type))
   AND upper(trim(cdr.drg)) = upper(trim(mapcl.drg))
   LEFT OUTER JOIN
     (SELECT DISTINCT apc_comp_dtl.schema_id,
                      apc_comp_dtl.mon_acct_payer_calc_summary_id,
                      apc_comp_dtl.ce_service_id,
                      apc_comp_dtl.ce_rule_id, -- APC_COMP_DTL.APC_CODE,
 apc_comp_dtl.apc_outlier_percent AS apc_outlier_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_apc_comp_dtl AS apc_comp_dtl
      UNION ALL SELECT DISTINCT mapc_apc.schema_id,
                                mapc_apc.mon_acct_payer_calc_summary_id,
                                mapc_apc.ce_service_id,
                                mapc_apc.ce_rule_id, -- MAPC_APC.APC_CODE,
 mapc_apc.apc_outlier_percent AS apc_outlier_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc AS mapc_apc) AS apc ON apc.schema_id = mapcl.schema_id
   AND apc.mon_acct_payer_calc_summary_id = mapcl.id
   AND apc.ce_service_id = mapcsvc.ce_service_id
   AND apc.ce_rule_id = mapcsvc.ce_rule_id
   LEFT OUTER JOIN
     (SELECT DISTINCT mapcsvc2.mon_acct_payer_calc_summary_id,
                      mapcsvc2.schema_id,
                      mapcsvc2.ce_service_id,
                      coin_fs2.cers_rate_amount AS opps_pct
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl2
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc2 ON mapcsvc2.schema_id = mapcl2.schema_id
      AND mapcsvc2.mon_acct_payer_calc_summary_id = mapcl2.id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_coin_fs AS coin_fs2 ON coin_fs2.schema_id = mapcl2.schema_id
      AND coin_fs2.mon_acct_payer_calc_summary_id = mapcl2.id
      AND coin_fs2.ce_service_id = mapcsvc2.ce_service_id
      WHERE mapcsvc2.ce_rule_type_id = 40 ) AS coin_fs ON coin_fs.schema_id = mapcl.schema_id
   AND coin_fs.mon_acct_payer_calc_summary_id = mapcl.id
   AND coin_fs.ce_service_id = mapcsvc.ce_service_id
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
(uuid, table_id, src_sys_nm, src_tbl_nm, tgt_tbl_nm, audit_type, 
expected_value, actual_value, load_start_time, load_end_time, 
load_run_time, job_name, audit_time, audit_status)
VALUES
(GENERATE_UUID(), cast(srctableid as int64), 'ra',srctablename, tgttablename, audit_type,
expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time, job_name, audit_time, audit_status
);

-- BEGIN TRANSACTION;


-- MERGE INTO {{ params.param_parallon_ra_audit_dataset_name }}.etl_control_expected AS mt USING
  -- (SELECT DISTINCT max(y.job_name) AS job_name,
                   -- y.job_start_date_time,
                   -- 1 AS control_id,
                   -- CAST(0 AS NUMERIC) AS control_tolerance_percent,
                   -- CAST(0 AS NUMERIC) AS control_tolerance_amt,
                   -- CAST(any_value(x.row_count) AS NUMERIC) AS control_value_expected
   -- FROM {{ params.param_parallon_ra_audit_dataset_name }}.etl_job_run AS y
   -- CROSS JOIN
     -- (SELECT count(*) AS ROW_COUNT
      -- FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg) AS x
   -- WHERE upper(trim(y.job_name)) = 'CTDRA401'
     -- AND y.job_start_date_time =
       -- (SELECT max(jr2.job_start_date_time)
        -- FROM {{ params.param_parallon_ra_audit_dataset_name }}.etl_job_run AS jr2
        -- WHERE upper(trim(jr2.job_name)) = 'CTDRA401'
          -- AND jr2.job_end_date_time IS NULL )
   -- GROUP BY upper(y.job_name),
            -- 2,
            -- 3,
            -- 4,
            -- 5) AS ms ON upper(coalesce(mt.job_name, '0')) = upper(coalesce(ms.job_name, '0'))
-- AND upper(coalesce(mt.job_name, '1')) = upper(coalesce(ms.job_name, '1'))
-- AND (coalesce(mt.job_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.job_start_date_time, DATETIME '1970-01-01 00:00:00')
     -- AND coalesce(mt.job_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.job_start_date_time, DATETIME '1970-01-01 00:00:01'))
-- AND (coalesce(mt.control_id, 0) = coalesce(ms.control_id, 0)
     -- AND coalesce(mt.control_id, 1) = coalesce(ms.control_id, 1))
-- AND (coalesce(mt.control_tolerance_percent, NUMERIC '0') = coalesce(ms.control_tolerance_percent, NUMERIC '0')
     -- AND coalesce(mt.control_tolerance_percent, NUMERIC '1') = coalesce(ms.control_tolerance_percent, NUMERIC '1'))
-- AND (coalesce(mt.control_tolerance_amt, NUMERIC '0') = coalesce(ms.control_tolerance_amt, NUMERIC '0')
     -- AND coalesce(mt.control_tolerance_amt, NUMERIC '1') = coalesce(ms.control_tolerance_amt, NUMERIC '1'))
-- AND (coalesce(mt.control_value_expected, NUMERIC '0') = coalesce(ms.control_value_expected, NUMERIC '0')
     -- AND coalesce(mt.control_value_expected, NUMERIC '1') = coalesce(ms.control_value_expected, NUMERIC '1')) WHEN NOT MATCHED BY TARGET THEN
-- INSERT (job_name,
        -- job_start_date_time,
        -- control_id,
        -- control_tolerance_percent,
        -- control_tolerance_amt,
        -- control_value_expected)
-- VALUES (ms.job_name, ms.job_start_date_time, ms.control_id, ms.control_tolerance_percent, ms.control_tolerance_amt, ms.control_value_expected);


-- SET DUP_COUNT =
  -- (SELECT count(*)
   -- FROM
     -- (SELECT job_name,
             -- job_start_date_time,
             -- control_id
      -- FROM {{ params.param_parallon_ra_audit_dataset_name }}.etl_control_expected
      -- GROUP BY job_name,
               -- job_start_date_time,
               -- control_id
      -- HAVING count(*) > 1));

-- IF DUP_COUNT <> 0 THEN
-- ROLLBACK TRANSACTION;

-- RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_cur_project_id }}`.{{ params.param_parallon_ra_audit_dataset_name }}.etl_control_expected');

-- ELSE
-- COMMIT TRANSACTION;

-- END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

-- No target-dialect support for source-dialect-specific DROP INDEX defn name
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation_service AS x USING
  (SELECT cc_eor_calculation_service_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_calculation_service_stg) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_code))
AND upper(trim(x.coid)) = upper(trim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.eor_log_date = z.eor_log_date
AND x.acct_payor_calc_service_id = z.acct_payor_calc_service_id WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    calc_id = z.calc_id,
    ce_service_id = z.ce_service_id,
    service_date = z.service_date,
    service_qty = z.service_qty,
    expected_payment_amt = z.expected_payment_amt,
    subterm_expected_payment_amt = z.subterm_expected_payment_amt,
    service_charge_amt = z.service_charge_amt,
    exclusion_charge_amt = z.exclusion_charge_amt,
    noncovered_charge_amt = z.noncovered_charge_amt,
    proc_code_type_desc = z.proc_code_type_desc,
    proc_code = z.proc_code,
    modifier_1_code = z.modifier_1_code,
    modifier_2_code = z.modifier_2_code,
    revenue_code_desc = z.revenue_code_desc,
    calculation_method_desc = z.calculation_method_desc,
    rate_info_from_day_num = z.rate_info_from_day_num,
    rate_info_to_day_num = z.rate_info_to_day_num,
    rate_info_amt = z.rate_info_amt,
    rate_info_service_desc = z.rate_info_service_desc,
    gov_drg_payment_amt = z.gov_drg_payment_amt,
    gov_ime_capital_drg_pmt_amt = z.gov_ime_capital_drg_pmt_amt,
    gov_operating_cost_outlier_amt = z.gov_operating_cost_outlier_amt,
    gov_operating_fed_pmt_amt = z.gov_operating_fed_pmt_amt,
    hipps_payment_amt = z.hipps_payment_amt,
    ipf_pps_outlier_per_diem_amt = z.ipf_pps_outlier_per_diem_amt,
    ipf_pps_out_per_diem_addon_amt = z.ipf_pps_out_per_diem_addon_amt,
    gov_scp_payment_amt = z.gov_scp_payment_amt,
    ipf_cost_charge_ratio_amt = z.ipf_cost_charge_ratio_amt,
    ipf_fixed_loss_amt = z.ipf_fixed_loss_amt,
    ipf_labor_share_amt = ROUND(z.ipf_labor_share_amt, 6, 'ROUND_HALF_EVEN'),
    ipf_labor_share_pct = z.ipf_labor_share_pct,
    ipf_non_labor_share_amt = z.ipf_non_labor_share_amt,
    ipf_teaching_adj_fctr_pct = z.ipf_teaching_adj_fctr_pct,
    ipf_age_fctr_pct = z.ipf_age_fctr_pct,
    ipf_drg_adj_fctr_pct = z.ipf_drg_adj_fctr_pct,
    ipf_comorbidity_fctr_pct = z.ipf_comorbidity_fctr_pct,
    ipf_pps_facility_adj_fctr_pct = z.ipf_pps_facility_adj_fctr_pct,
    ipf_pps_adj_fctr_pct = z.ipf_pps_adj_fctr_pct,
    ipf_pps_los_adj_fctr_pct = ROUND(z.ipf_pps_los_adj_fctr_pct, 4, 'ROUND_HALF_EVEN'),
    gov_new_tech_add_on_amt = z.gov_new_tech_add_on_amt,
    gov_device_offset_red_amt = z.gov_device_offset_red_amt,
    gov_transfer_pct = z.gov_transfer_pct,
    ipf_per_diem_day_num = z.ipf_per_diem_day_num,
    apc_composite_flg = z.apc_composite_flg,
    dsh_unadj_opr_add_on_amt = z.dsh_unadj_opr_add_on_amt,
    dsh_uncmps_care_add_on_amt = z.dsh_uncmps_care_add_on_amt,
    mdh_add_on_amt = z.mdh_add_on_amt,
    low_vol_add_on_amt = z.low_vol_add_on_amt,
    readm_adj_amt = z.readm_adj_amt,
    val_based_adj_amt = z.val_based_adj_amt,
    drg_fixed_loss_threshold_amt = z.drg_fixed_loss_threshold_amt,
    drg_marginal_cost_fctr_pct = z.drg_marginal_cost_fctr_pct,
    apc_outlier_pct = z.apc_outlier_pct,
    opps_pct = z.opps_pct,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (coid,
        company_code,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        eor_log_date,
        acct_payor_calc_service_id,
        calc_id,
        ce_service_id,
        service_date,
        service_qty,
        expected_payment_amt,
        subterm_expected_payment_amt,
        service_charge_amt,
        exclusion_charge_amt,
        noncovered_charge_amt,
        proc_code_type_desc,
        proc_code,
        modifier_1_code,
        modifier_2_code,
        revenue_code_desc,
        calculation_method_desc,
        rate_info_from_day_num,
        rate_info_to_day_num,
        rate_info_amt,
        rate_info_service_desc,
        gov_drg_payment_amt,
        gov_ime_capital_drg_pmt_amt,
        gov_operating_cost_outlier_amt,
        gov_operating_fed_pmt_amt,
        hipps_payment_amt,
        ipf_pps_outlier_per_diem_amt,
        ipf_pps_out_per_diem_addon_amt,
        gov_scp_payment_amt,
        ipf_cost_charge_ratio_amt,
        ipf_fixed_loss_amt,
        ipf_labor_share_amt,
        ipf_labor_share_pct,
        ipf_non_labor_share_amt,
        ipf_teaching_adj_fctr_pct,
        ipf_age_fctr_pct,
        ipf_drg_adj_fctr_pct,
        ipf_comorbidity_fctr_pct,
        ipf_pps_facility_adj_fctr_pct,
        ipf_pps_adj_fctr_pct,
        ipf_pps_los_adj_fctr_pct,
        gov_new_tech_add_on_amt,
        gov_device_offset_red_amt,
        gov_transfer_pct,
        ipf_per_diem_day_num,
        apc_composite_flg,
        dsh_unadj_opr_add_on_amt,
        dsh_uncmps_care_add_on_amt,
        mdh_add_on_amt,
        low_vol_add_on_amt,
        readm_adj_amt,
        val_based_adj_amt,
        drg_fixed_loss_threshold_amt,
        drg_marginal_cost_fctr_pct,
        apc_outlier_pct,
        opps_pct,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.coid, z.company_code, z.patient_dw_id, z.payor_dw_id, z.iplan_insurance_order_num, z.unit_num, z.pat_acct_num, z.iplan_id, z.eor_log_date, z.acct_payor_calc_service_id, z.calc_id, z.ce_service_id, z.service_date, z.service_qty, z.expected_payment_amt, z.subterm_expected_payment_amt, z.service_charge_amt, z.exclusion_charge_amt, z.noncovered_charge_amt, z.proc_code_type_desc, z.proc_code, z.modifier_1_code, z.modifier_2_code, z.revenue_code_desc, z.calculation_method_desc, z.rate_info_from_day_num, z.rate_info_to_day_num, z.rate_info_amt, z.rate_info_service_desc, z.gov_drg_payment_amt, z.gov_ime_capital_drg_pmt_amt, z.gov_operating_cost_outlier_amt, z.gov_operating_fed_pmt_amt, z.hipps_payment_amt, z.ipf_pps_outlier_per_diem_amt, z.ipf_pps_out_per_diem_addon_amt, z.gov_scp_payment_amt, z.ipf_cost_charge_ratio_amt, z.ipf_fixed_loss_amt, ROUND(z.ipf_labor_share_amt, 6, 'ROUND_HALF_EVEN'), z.ipf_labor_share_pct, z.ipf_non_labor_share_amt, z.ipf_teaching_adj_fctr_pct, z.ipf_age_fctr_pct, z.ipf_drg_adj_fctr_pct, z.ipf_comorbidity_fctr_pct, z.ipf_pps_facility_adj_fctr_pct, z.ipf_pps_adj_fctr_pct, ROUND(z.ipf_pps_los_adj_fctr_pct, 4, 'ROUND_HALF_EVEN'), z.gov_new_tech_add_on_amt, z.gov_device_offset_red_amt, z.gov_transfer_pct, z.ipf_per_diem_day_num, z.apc_composite_flg, z.dsh_unadj_opr_add_on_amt, z.dsh_uncmps_care_add_on_amt, z.mdh_add_on_amt, z.low_vol_add_on_amt, z.readm_adj_amt, z.val_based_adj_amt, z.drg_fixed_loss_threshold_amt, z.drg_marginal_cost_fctr_pct, z.apc_outlier_pct, z.opps_pct, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             acct_payor_calc_service_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation_service
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               acct_payor_calc_service_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation_service');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation_service
WHERE upper(cc_eor_calculation_service.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(trim(r.org_status)) = 'ACTIVE' )
  AND cc_eor_calculation_service.dw_last_update_date_time <>
    (SELECT max(cc_eor_calculation_service_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_calculation_service AS cc_eor_calculation_service_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

-- No target-dialect support for source-dialect-specific CREATE INDEX
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_EOR_Calculation_Service');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;