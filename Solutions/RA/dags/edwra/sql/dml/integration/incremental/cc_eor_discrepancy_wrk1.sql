DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_discrepancy_wrk1.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**************************************************************************************************
   Developer: Faiyaz
        Date: 9/29/2011
        Name: EOR_Discrepancy_Wrk1.sql
     Purpose: Builds the EOR_Discrepancy_Wrk1 staging table used within the Business Objects
	          AD-HOC Universe for reporting.
        Mod1: Based on revised EOR model.
        Mod2: Added Reason_Code_3 on 07/21/2014 MFY
        Mod3: Added EOR_Total_Actual_Pat_Resp_Amt on 01/29/2015 AP
        Mod4: Added EOR_HAC_Adj_Amt on 06/22/2015 AP
        Mod5: Added COID join for Ref_CC_Account_Payer_Stts table 03/15/2016 JC
		Mod6: Update for PBI11616 Populate Discrepancy Reason 3 for DRT and PMT Variance Letter Activity.
		      Added QUERY_BAND per Teradata DBA standards on 11/7/2017 SW.
		Mod7: Added join from EDWRA_Base_Views.cc_eor to Ref_CC_Org_Structure to get active coids
              on 1/31/2018 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
***************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA234_EOR_Disc_Wrk1;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS mt USING
  (SELECT DISTINCT cceor.company_code AS company_code,
                   cceor.coid AS coid,
                   cceor.patient_dw_id,
                   cceor.payor_dw_id,
                   cceor.iplan_insurance_order_num,
                   cceor.eor_log_date,
                   cceor.log_id AS log_id,
                   cceor.log_sequence_num,
                   cceor.eff_from_date,
                   cceor.unit_num AS unit_num,
                   cceor.pat_acct_num,
                   cceor.iplan_id,
                   cceor.ar_bill_thru_date,
                   ccaa.activity_due_date,
                   cceor.cc_drg_code AS cc_drg_code,
                   cceor.eor_hipps_code AS eor_hipps_code,
                   coalesce(cceorc.length_of_stay_days_num, 0) AS length_of_stay_days_num,
                   CAST(ROUND(coalesce(cceor.eor_net_reimbursement_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_payment_amt,
                   CAST(ROUND(coalesce(cceor.eor_contractual_allowance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_contractual_allowance_amt,
                   coalesce(cceor.eor_covered_days_num, 0) AS eor_covered_days_num,
                   CAST(ROUND(coalesce(cceor.eor_deductible_amt, NUMERIC '0') + coalesce(cceor.eor_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_deductible_coinsurance_amt,
                   CAST(ROUND(coalesce(cceor.eor_gross_reimbursement_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_gross_reimbursement_amt,
                   CAST(ROUND(coalesce(cceor.sqstrtn_red_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_sqstrtn_red_amt,
                   CAST(ROUND(coalesce(cceor.eor_total_charge_amt, NUMERIC '0') - coalesce(cceor.eor_non_covered_charge_amt, NUMERIC '0') - coalesce(cceor.eor_non_bill_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_covered_charge_amt,
                   CAST(ROUND(coalesce(cceor.eor_non_covered_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_non_covered_charge_amt,
                   CAST(ROUND(coalesce(cceor.eor_total_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_total_charge_amt,
                   CAST(ROUND(coalesce(cceor.eor_insurance_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_insurance_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_blood_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_blood_deductible_amt,
                   CAST(ROUND(coalesce(cceora.eor_capital_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_capital_payment_amt,
                   CAST(ROUND(coalesce(cceor.eor_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_coinsurance_amt,
                   CAST(ROUND(coalesce(cceor.eor_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_deductible_amt,
                   CAST(ROUND(coalesce(cceor.total_actual_pat_resp_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_total_actual_pat_resp_amt,
                   CAST(ROUND(coalesce(cceora.eor_lab_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_lab_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_net_billed_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_net_billed_charge_amt,
                   CAST(ROUND(coalesce(cceora.eor_outlier_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_outlier_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_primary_payor_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_primary_payor_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_therapy_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_therapy_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_copay_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_copay_amt,
                   CAST(ROUND(coalesce(cceora.pa_total_account_balance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS pa_total_account_balance_amt,
                   CAST(ROUND(coalesce(cceora.eor_net_apc_service_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_net_apc_service_amt,
                   CAST(ROUND(coalesce(cceora.eor_net_fee_schedule_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_net_fee_schedule_amt,
                   CAST(ROUND(coalesce(cceora.eor_total_variance_adj_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_total_variance_adj_amt,
                   CAST(ROUND(coalesce(cceora.eor_total_trans_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_total_trans_payment_amt,
                   CAST(ROUND(coalesce(cceora.eor_hac_adj_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_hac_adj_amt,
                   CAST(ROUND(coalesce(cceora.total_denial_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS total_denial_amt,
                   cceor.inpatient_outpatient_ind AS inpatient_outpatient_ind,
                   cceor.interim_bill_ind AS interim_bill_ind,
                   substr(CASE
                              WHEN substr(cceor.project_desc, 1, 3) IS NULL THEN CASE upper(rtrim(upper(regexp_replace(ccact.activity_subject_text, ' ', ''))))
                                                                                     WHEN upper(rtrim(upper(regexp_replace('=> Next Action Due 30 Days DRT Ltr1', ' ', '')))) THEN '882'
                                                                                     WHEN upper(rtrim(upper(regexp_replace('=>Next Action Due 30 Days DRT Ltr2', ' ', '')))) THEN '883'
                                                                                     WHEN upper(rtrim(upper(regexp_replace('=>Next Action Due 30 Days PMT Ltr 1', ' ', '')))) THEN '885'
                                                                                     WHEN upper(rtrim(upper(regexp_replace('=>Next Action Due 30 Days PMT Ltr 2', ' ', '')))) THEN '886'
                                                                                     ELSE CAST(NULL AS STRING)
                                                                                 END
                              ELSE substr(cceor.project_desc, 1, 3)
                          END, 1, 3) AS reason_code_3,
                   cceor.reason_id,
                   cceor.calc_id,
                   ROUND(ccaa.activity_id, 0, 'ROUND_HALF_EVEN') AS account_activity_id,
                   cceorc.account_payer_status_id,
                   stts.status_phase_id,
                   stts.status_category_id,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account AS cca
   INNER JOIN
     (SELECT a.company_code,
             a.coid,
             a.patient_dw_id,
             a.payor_dw_id,
             a.iplan_insurance_order_num,
             a.eor_log_date,
             a.log_id,
             a.log_sequence_num,
             a.eff_from_date,
             a.unit_num,
             a.pat_acct_num,
             a.iplan_id,
             a.financial_class_code,
             a.final_bill_date,
             a.ar_bill_thru_date,
             a.ar_bill_drop_date,
             a.contract_begin_date,
             a.eor_transaction_date,
             a.bill_reason_code,
             a.eor_drg_code,
             a.cc_drg_code,
             a.cc_drg_weight,
             a.eor_hipps_code,
             a.cc_cmg_code,
             a.cc_cmg_weight,
             a.eor_covered_days_num,
             a.eor_total_charge_amt,
             a.expected_insurance_payment_amt,
             a.pat_resp_non_covered_chg_amt,
             a.total_actual_pat_resp_amt,
             a.eor_non_covered_charge_amt,
             a.eor_cmptd_reimbursement_amt,
             a.eor_deductible_amt,
             a.eor_coinsurance_amt,
             a.eor_copay_amt,
             a.eor_insurance_payment_amt,
             a.eor_contractual_allowance_amt,
             a.eor_cost_of_service_amt,
             a.month_13_ind,
             a.outlier_code,
             a.auto_post_amt,
             a.auto_post_ind,
             a.drg_table_id,
             a.opps_ind,
             a.inpatient_outpatient_ind,
             a.visit_count,
             a.eor_gross_reimbursement_amt,
             a.eor_net_reimbursement_amt,
             a.eor_non_bill_charge_amt,
             a.sqstrtn_red_amt,
             a.model_name,
             a.project_desc,
             a.eor_ipf_interrupted_day_stay,
             a.calc_success_ind,
             a.latest_calc_ind,
             a.interim_bill_ind,
             a.financial_period_id,
             a.reason_id,
             a.calc_id,
             a.source_system_code,
             a.dw_last_update_date_time
      FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor AS a
      INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON upper(rtrim(a.company_code)) = upper(rtrim(ros.company_code))
      AND upper(rtrim(a.coid)) = upper(rtrim(ros.coid))
      AND upper(rtrim(ros.org_status)) = 'ACTIVE'
      WHERE a.eor_log_date =
          (SELECT max(b.eor_log_date)
           FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor AS b
           WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
             AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
             AND a.patient_dw_id = b.patient_dw_id
             AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
             AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
             AND a.log_sequence_num = b.log_sequence_num ) ) AS cceor ON cca.patient_dw_id = cceor.patient_dw_id
   LEFT OUTER JOIN
     (SELECT a.patient_dw_id,
             min(b.activity_id) AS activity_id,
             b.activity_due_date,
             b.activity_complete_date_time
      FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account AS a
      INNER JOIN   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS b ON a.patient_dw_id = b.patient_dw_id
      WHERE b.activity_complete_date_time IS NULL
        AND b.activity_due_date IS NOT NULL
        AND b.activity_create_date_time IS NOT NULL
        AND upper(rtrim(b.active_ind)) = 'Y'
        AND upper(rtrim(b.activity_source_code)) IN('A',
                                                    'W')
        AND b.activity_due_date =
          (SELECT min(c.activity_due_date)
           FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS c
           WHERE a.patient_dw_id = c.patient_dw_id
             AND c.activity_due_date IS NOT NULL
             AND c.activity_create_date_time IS NOT NULL
             AND c.activity_complete_date_time IS NULL
             AND upper(rtrim(c.active_ind)) = 'Y'
             AND upper(rtrim(c.activity_source_code)) IN('A',
                                                         'W') )
      GROUP BY 1,
               3,
               4) AS ccaa ON cca.patient_dw_id = ccaa.patient_dw_id
   LEFT OUTER JOIN --  New Logic PBI11616 --

     (SELECT acct_act.patient_dw_id,
             acct_act.payor_dw_id,
             acct_act.iplan_id,
             acct_act.iplan_insurance_order_num,
             acct_act.activity_complete_date_time,
             acct_act.activity_id,
             acct_act.activity_subject_text
      FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS acct_act
      INNER JOIN
        (SELECT t.patient_dw_id,
                max(t.activity_id) AS activity_id
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_account_activity AS t
         WHERE t.activity_type_id > 10000
           AND t.activity_complete_date_time IS NULL
         GROUP BY 1) AS vtl ON vtl.patient_dw_id = acct_act.patient_dw_id
      AND vtl.activity_id = acct_act.activity_id) AS ccact ON cceor.patient_dw_id = ccact.patient_dw_id
   AND cceor.payor_dw_id = ccact.payor_dw_id
   AND cceor.iplan_insurance_order_num = ccact.iplan_insurance_order_num
   LEFT OUTER JOIN --  End of New Logic PBI11616 --
   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_calculation AS cceorc ON upper(rtrim(cceor.company_code)) = upper(rtrim(cceorc.company_code))
   AND upper(rtrim(cceor.coid)) = upper(rtrim(cceorc.coid))
   AND cceor.patient_dw_id = cceorc.patient_dw_id
   AND cceor.payor_dw_id = cceorc.payor_dw_id
   AND cceor.iplan_insurance_order_num = cceorc.iplan_insurance_order_num
   AND cceor.eor_log_date = cceorc.eor_log_date
   AND upper(rtrim(cceor.log_id)) = upper(rtrim(cceorc.log_id))
   AND cceor.log_sequence_num = cceorc.log_sequence_num
   AND cceor.eff_from_date = cceorc.eff_from_date
   LEFT OUTER JOIN   {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_account_payer_stts AS stts ON upper(rtrim(cceorc.company_code)) = upper(rtrim(stts.company_code))
   AND cceorc.account_payer_status_id = stts.status_id
   LEFT OUTER JOIN
     (SELECT dt_cceor.company_code,
             dt_cceor.coid,
             dt_cceor.patient_dw_id,
             dt_cceor.payor_dw_id,
             dt_cceor.iplan_insurance_order_num,
             dt_cceor.eor_log_date,
             dt_cceor.log_id,
             dt_cceor.log_sequence_num,
             dt_cceor.eff_from_date,
             dt_cceor.unit_num,
             dt_cceor.pat_acct_num,
             dt_cceor.iplan_id,
             coalesce(bda.eor_blood_deductible_amt, NUMERIC '0') AS eor_blood_deductible_amt,
             coalesce(cpa.eor_capital_payment_amt, NUMERIC '0') AS eor_capital_payment_amt,
             coalesce(lpa.eor_lab_payment_amt, NUMERIC '0') AS eor_lab_payment_amt,
             coalesce(nbca.eor_net_billed_charge_amt, NUMERIC '0') AS eor_net_billed_charge_amt,
             coalesce(opa.eor_outlier_payment_amt, NUMERIC '0') AS eor_outlier_payment_amt,
             coalesce(pppa.eor_primary_payor_payment_amt, NUMERIC '0') AS eor_primary_payor_payment_amt,
             coalesce(tpa.eor_therapy_payment_amt, NUMERIC '0') AS eor_therapy_payment_amt,
             coalesce(ca.eor_copay_amt, NUMERIC '0') AS eor_copay_amt,
             coalesce(ptaba.pa_total_account_balance_amt, NUMERIC '0') AS pa_total_account_balance_amt,
             coalesce(nasa.eor_net_apc_service_amt, NUMERIC '0') AS eor_net_apc_service_amt,
             coalesce(nfsa.eor_net_fee_schedule_amt, NUMERIC '0') AS eor_net_fee_schedule_amt,
             coalesce(ttpa.eor_total_trans_payment_amt, NUMERIC '0') AS eor_total_trans_payment_amt,
             coalesce(tva.eor_total_variance_adj_amt, NUMERIC '0') AS eor_total_variance_adj_amt,
             coalesce(tda.total_denial_amt, NUMERIC '0') AS total_denial_amt,
             coalesce(hac.eor_hac_adj_amt, NUMERIC '0') AS eor_hac_adj_amt
      FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor AS dt_cceor
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_blood_deductible_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 32
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS bda ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(bda.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(bda.coid))
      AND dt_cceor.patient_dw_id = bda.patient_dw_id
      AND dt_cceor.payor_dw_id = bda.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = bda.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = bda.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(bda.log_id))
      AND dt_cceor.log_sequence_num = bda.log_sequence_num
      AND dt_cceor.eff_from_date = bda.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_capital_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 8
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS cpa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(cpa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(cpa.coid))
      AND dt_cceor.patient_dw_id = cpa.patient_dw_id
      AND dt_cceor.payor_dw_id = cpa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = cpa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = cpa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(cpa.log_id))
      AND dt_cceor.log_sequence_num = cpa.log_sequence_num
      AND dt_cceor.eff_from_date = cpa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_lab_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 50
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS lpa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(lpa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(lpa.coid))
      AND dt_cceor.patient_dw_id = lpa.patient_dw_id
      AND dt_cceor.payor_dw_id = lpa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = lpa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = lpa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(lpa.log_id))
      AND dt_cceor.log_sequence_num = lpa.log_sequence_num
      AND dt_cceor.eff_from_date = lpa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_net_billed_charge_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 114
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS nbca ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(nbca.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(nbca.coid))
      AND dt_cceor.patient_dw_id = nbca.patient_dw_id
      AND dt_cceor.payor_dw_id = nbca.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = nbca.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = nbca.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(nbca.log_id))
      AND dt_cceor.log_sequence_num = nbca.log_sequence_num
      AND dt_cceor.eff_from_date = nbca.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_outlier_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 71
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS opa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(opa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(opa.coid))
      AND dt_cceor.patient_dw_id = opa.patient_dw_id
      AND dt_cceor.payor_dw_id = opa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = opa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = opa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(opa.log_id))
      AND dt_cceor.log_sequence_num = opa.log_sequence_num
      AND dt_cceor.eff_from_date = opa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_primary_payor_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 133
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS pppa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(pppa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(pppa.coid))
      AND dt_cceor.patient_dw_id = pppa.patient_dw_id
      AND dt_cceor.payor_dw_id = pppa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = pppa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = pppa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(pppa.log_id))
      AND dt_cceor.log_sequence_num = pppa.log_sequence_num
      AND dt_cceor.eff_from_date = pppa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_therapy_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 93
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS tpa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(tpa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(tpa.coid))
      AND dt_cceor.patient_dw_id = tpa.patient_dw_id
      AND dt_cceor.payor_dw_id = tpa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = tpa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = tpa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(tpa.log_id))
      AND dt_cceor.log_sequence_num = tpa.log_sequence_num
      AND dt_cceor.eff_from_date = tpa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_copay_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 123
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS ca ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(ca.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(ca.coid))
      AND dt_cceor.patient_dw_id = ca.patient_dw_id
      AND dt_cceor.payor_dw_id = ca.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = ca.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = ca.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(ca.log_id))
      AND dt_cceor.log_sequence_num = ca.log_sequence_num
      AND dt_cceor.eff_from_date = ca.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS pa_total_account_balance_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 134
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS ptaba ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(ptaba.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(ptaba.coid))
      AND dt_cceor.patient_dw_id = ptaba.patient_dw_id
      AND dt_cceor.payor_dw_id = ptaba.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = ptaba.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = ptaba.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(ptaba.log_id))
      AND dt_cceor.log_sequence_num = ptaba.log_sequence_num
      AND dt_cceor.eff_from_date = ptaba.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_net_apc_service_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 135
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS nasa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(nasa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(nasa.coid))
      AND dt_cceor.patient_dw_id = nasa.patient_dw_id
      AND dt_cceor.payor_dw_id = nasa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = nasa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = nasa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(nasa.log_id))
      AND dt_cceor.log_sequence_num = nasa.log_sequence_num
      AND dt_cceor.eff_from_date = nasa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_net_fee_schedule_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 136
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS nfsa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(nfsa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(nfsa.coid))
      AND dt_cceor.patient_dw_id = nfsa.patient_dw_id
      AND dt_cceor.payor_dw_id = nfsa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = nfsa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = nfsa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(nfsa.log_id))
      AND dt_cceor.log_sequence_num = nfsa.log_sequence_num
      AND dt_cceor.eff_from_date = nfsa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_total_trans_payment_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 117
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS ttpa ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(ttpa.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(ttpa.coid))
      AND dt_cceor.patient_dw_id = ttpa.patient_dw_id
      AND dt_cceor.payor_dw_id = ttpa.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = ttpa.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = ttpa.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(ttpa.log_id))
      AND dt_cceor.log_sequence_num = ttpa.log_sequence_num
      AND dt_cceor.eff_from_date = ttpa.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_total_variance_adj_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 124
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS tva ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(tva.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(tva.coid))
      AND dt_cceor.patient_dw_id = tva.patient_dw_id
      AND dt_cceor.payor_dw_id = tva.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = tva.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = tva.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(tva.log_id))
      AND dt_cceor.log_sequence_num = tva.log_sequence_num
      AND dt_cceor.eff_from_date = tva.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS total_denial_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 140
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS tda ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(tda.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(tda.coid))
      AND dt_cceor.patient_dw_id = tda.patient_dw_id
      AND dt_cceor.payor_dw_id = tda.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = tda.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = tda.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(tda.log_id))
      AND dt_cceor.log_sequence_num = tda.log_sequence_num
      AND dt_cceor.eff_from_date = tda.eff_from_date
      LEFT OUTER JOIN
        (SELECT a.company_code,
                a.coid,
                a.patient_dw_id,
                a.payor_dw_id,
                a.iplan_insurance_order_num,
                a.eor_log_date,
                a.log_id,
                a.log_sequence_num,
                a.eff_from_date,
                a.eor_reimbursement_amt AS eor_hac_adj_amt
         FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS a
         WHERE a.amount_category_code = 45
           AND a.dw_last_update_date_time =
             (SELECT max(b.dw_last_update_date_time)
              FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_amount AS b
              WHERE upper(rtrim(a.company_code)) = upper(rtrim(b.company_code))
                AND upper(rtrim(a.coid)) = upper(rtrim(b.coid))
                AND a.patient_dw_id = b.patient_dw_id
                AND a.payor_dw_id = b.payor_dw_id
                AND a.iplan_insurance_order_num = b.iplan_insurance_order_num
                AND a.eor_log_date = b.eor_log_date
                AND upper(rtrim(a.log_id)) = upper(rtrim(b.log_id))
                AND a.log_sequence_num = b.log_sequence_num
                AND a.eff_from_date = b.eff_from_date
                AND a.amount_category_code = b.amount_category_code ) ) AS hac ON upper(rtrim(dt_cceor.company_code)) = upper(rtrim(hac.company_code))
      AND upper(rtrim(dt_cceor.coid)) = upper(rtrim(hac.coid))
      AND dt_cceor.patient_dw_id = hac.patient_dw_id
      AND dt_cceor.payor_dw_id = hac.payor_dw_id
      AND dt_cceor.iplan_insurance_order_num = hac.iplan_insurance_order_num
      AND dt_cceor.eor_log_date = hac.eor_log_date
      AND upper(rtrim(dt_cceor.log_id)) = upper(rtrim(hac.log_id))
      AND dt_cceor.log_sequence_num = hac.log_sequence_num
      AND dt_cceor.eff_from_date = hac.eff_from_date) AS cceora ON upper(rtrim(cceor.company_code)) = upper(rtrim(cceora.company_code))
   AND upper(rtrim(cceor.coid)) = upper(rtrim(cceora.coid))
   AND cceor.patient_dw_id = cceora.patient_dw_id
   AND cceor.payor_dw_id = cceora.payor_dw_id
   AND cceor.iplan_insurance_order_num = cceora.iplan_insurance_order_num
   AND cceor.eor_log_date = cceora.eor_log_date
   AND upper(rtrim(cceor.log_id)) = upper(rtrim(cceora.log_id))
   AND cceor.log_sequence_num = cceora.log_sequence_num
   AND cceor.eff_from_date = cceora.eff_from_date
   WHERE (upper(rtrim(cca.billing_status_code)) = 'B'
          OR upper(rtrim(cca.cancel_bill_ind)) IN('Y',
                                                  'C'))
     AND DATE(cca.admission_date_time) > DATE '2011-09-01'
     AND cceor.iplan_id <> 0
     AND cceor.iplan_insurance_order_num < 51
     AND stts.status_category_id = 2
     AND EXISTS
       (SELECT 1
        FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance AS ccr
        WHERE coalesce(ccr.ra_claim_status_code, 0) <> 4
          AND upper(rtrim(ccr.active_ind)) = 'Y'
          AND cceor.patient_dw_id = ccr.patient_dw_id
          AND cceor.payor_dw_id = ccr.payor_dw_id
          AND upper(CAST(ccr.dw_last_update_date_time AS STRING)) IN
            (SELECT upper(CAST(max(ccrm.dw_last_update_date_time) AS STRING))
             FROM   {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance AS ccrm
             WHERE ccrm.patient_dw_id = ccr.patient_dw_id ) )
     AND (cceor.eor_insurance_payment_amt - cceora.eor_total_variance_adj_amt - cceora.eor_total_trans_payment_amt - cceora.total_denial_amt <> 0
          OR stts.status_phase_id = 2666703) ) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.eor_log_date, DATE '1970-01-01') = coalesce(ms.eor_log_date, DATE '1970-01-01')
     AND coalesce(mt.eor_log_date, DATE '1970-01-02') = coalesce(ms.eor_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.eff_from_date, DATE '1970-01-01') = coalesce(ms.eff_from_date, DATE '1970-01-01')
     AND coalesce(mt.eff_from_date, DATE '1970-01-02') = coalesce(ms.eff_from_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (coalesce(mt.work_date, DATE '1970-01-01') = coalesce(ms.activity_due_date, DATE '1970-01-01')
     AND coalesce(mt.work_date, DATE '1970-01-02') = coalesce(ms.activity_due_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.cc_drg_code, '0')) = upper(coalesce(ms.cc_drg_code, '0'))
     AND upper(coalesce(mt.cc_drg_code, '1')) = upper(coalesce(ms.cc_drg_code, '1')))
AND (upper(coalesce(mt.eor_hipps_code, '0')) = upper(coalesce(ms.eor_hipps_code, '0'))
     AND upper(coalesce(mt.eor_hipps_code, '1')) = upper(coalesce(ms.eor_hipps_code, '1')))
AND (coalesce(mt.length_of_stay_days_num, 0) = coalesce(ms.length_of_stay_days_num, 0)
     AND coalesce(mt.length_of_stay_days_num, 1) = coalesce(ms.length_of_stay_days_num, 1))
AND (coalesce(mt.eor_payment_amt, NUMERIC '0') = coalesce(ms.eor_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_payment_amt, NUMERIC '1') = coalesce(ms.eor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_contractual_allowance_amt, NUMERIC '0') = coalesce(ms.eor_contractual_allowance_amt, NUMERIC '0')
     AND coalesce(mt.eor_contractual_allowance_amt, NUMERIC '1') = coalesce(ms.eor_contractual_allowance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_covered_days_num, 0) = coalesce(ms.eor_covered_days_num, 0)
     AND coalesce(mt.eor_covered_days_num, 1) = coalesce(ms.eor_covered_days_num, 1))
AND (coalesce(mt.eor_deductible_coinsurance_amt, NUMERIC '0') = coalesce(ms.eor_deductible_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.eor_deductible_coinsurance_amt, NUMERIC '1') = coalesce(ms.eor_deductible_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_gross_reimbursement_amt, NUMERIC '0') = coalesce(ms.eor_gross_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.eor_gross_reimbursement_amt, NUMERIC '1') = coalesce(ms.eor_gross_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.eor_sqstrtn_red_amt, NUMERIC '0') = coalesce(ms.eor_sqstrtn_red_amt, NUMERIC '0')
     AND coalesce(mt.eor_sqstrtn_red_amt, NUMERIC '1') = coalesce(ms.eor_sqstrtn_red_amt, NUMERIC '1'))
AND (coalesce(mt.eor_covered_charge_amt, NUMERIC '0') = coalesce(ms.eor_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_covered_charge_amt, NUMERIC '1') = coalesce(ms.eor_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.eor_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.eor_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_total_charge_amt, NUMERIC '0') = coalesce(ms.eor_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_total_charge_amt, NUMERIC '1') = coalesce(ms.eor_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_insurance_payment_amt, NUMERIC '0') = coalesce(ms.eor_insurance_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_insurance_payment_amt, NUMERIC '1') = coalesce(ms.eor_insurance_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_blood_deductible_amt, NUMERIC '0') = coalesce(ms.eor_blood_deductible_amt, NUMERIC '0')
     AND coalesce(mt.eor_blood_deductible_amt, NUMERIC '1') = coalesce(ms.eor_blood_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.eor_capital_payment_amt, NUMERIC '0') = coalesce(ms.eor_capital_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_capital_payment_amt, NUMERIC '1') = coalesce(ms.eor_capital_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_coinsurance_amt, NUMERIC '0') = coalesce(ms.eor_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.eor_coinsurance_amt, NUMERIC '1') = coalesce(ms.eor_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_deductible_amt, NUMERIC '0') = coalesce(ms.eor_deductible_amt, NUMERIC '0')
     AND coalesce(mt.eor_deductible_amt, NUMERIC '1') = coalesce(ms.eor_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.eor_total_actual_pat_resp_amt, NUMERIC '0') = coalesce(ms.eor_total_actual_pat_resp_amt, NUMERIC '0')
     AND coalesce(mt.eor_total_actual_pat_resp_amt, NUMERIC '1') = coalesce(ms.eor_total_actual_pat_resp_amt, NUMERIC '1'))
AND (coalesce(mt.eor_lab_payment_amt, NUMERIC '0') = coalesce(ms.eor_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_lab_payment_amt, NUMERIC '1') = coalesce(ms.eor_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_net_billed_charge_amt, NUMERIC '0') = coalesce(ms.eor_net_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_net_billed_charge_amt, NUMERIC '1') = coalesce(ms.eor_net_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_outlier_payment_amt, NUMERIC '0') = coalesce(ms.eor_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_outlier_payment_amt, NUMERIC '1') = coalesce(ms.eor_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_primary_payor_payment_amt, NUMERIC '0') = coalesce(ms.eor_primary_payor_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_primary_payor_payment_amt, NUMERIC '1') = coalesce(ms.eor_primary_payor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_therapy_payment_amt, NUMERIC '0') = coalesce(ms.eor_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_therapy_payment_amt, NUMERIC '1') = coalesce(ms.eor_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_copay_amt, NUMERIC '0') = coalesce(ms.eor_copay_amt, NUMERIC '0')
     AND coalesce(mt.eor_copay_amt, NUMERIC '1') = coalesce(ms.eor_copay_amt, NUMERIC '1'))
AND (coalesce(mt.pa_total_account_balance_amt, NUMERIC '0') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '0')
     AND coalesce(mt.pa_total_account_balance_amt, NUMERIC '1') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_net_apc_service_amt, NUMERIC '0') = coalesce(ms.eor_net_apc_service_amt, NUMERIC '0')
     AND coalesce(mt.eor_net_apc_service_amt, NUMERIC '1') = coalesce(ms.eor_net_apc_service_amt, NUMERIC '1'))
AND (coalesce(mt.eor_net_fee_schedule_amt, NUMERIC '0') = coalesce(ms.eor_net_fee_schedule_amt, NUMERIC '0')
     AND coalesce(mt.eor_net_fee_schedule_amt, NUMERIC '1') = coalesce(ms.eor_net_fee_schedule_amt, NUMERIC '1'))
AND (coalesce(mt.eor_total_variance_adj_amt, NUMERIC '0') = coalesce(ms.eor_total_variance_adj_amt, NUMERIC '0')
     AND coalesce(mt.eor_total_variance_adj_amt, NUMERIC '1') = coalesce(ms.eor_total_variance_adj_amt, NUMERIC '1'))
AND (coalesce(mt.eor_total_trans_payment_amt, NUMERIC '0') = coalesce(ms.eor_total_trans_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_total_trans_payment_amt, NUMERIC '1') = coalesce(ms.eor_total_trans_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_hac_adj_amt, NUMERIC '0') = coalesce(ms.eor_hac_adj_amt, NUMERIC '0')
     AND coalesce(mt.eor_hac_adj_amt, NUMERIC '1') = coalesce(ms.eor_hac_adj_amt, NUMERIC '1'))
AND (coalesce(mt.total_denial_amt, NUMERIC '0') = coalesce(ms.total_denial_amt, NUMERIC '0')
     AND coalesce(mt.total_denial_amt, NUMERIC '1') = coalesce(ms.total_denial_amt, NUMERIC '1'))
AND (upper(coalesce(mt.inpatient_outpatient_ind, '0')) = upper(coalesce(ms.inpatient_outpatient_ind, '0'))
     AND upper(coalesce(mt.inpatient_outpatient_ind, '1')) = upper(coalesce(ms.inpatient_outpatient_ind, '1')))
AND (upper(coalesce(mt.interim_bill_ind, '0')) = upper(coalesce(ms.interim_bill_ind, '0'))
     AND upper(coalesce(mt.interim_bill_ind, '1')) = upper(coalesce(ms.interim_bill_ind, '1')))
AND (upper(coalesce(mt.reason_code_3, '0')) = upper(coalesce(ms.reason_code_3, '0'))
     AND upper(coalesce(mt.reason_code_3, '1')) = upper(coalesce(ms.reason_code_3, '1')))
AND (coalesce(mt.reason_id, NUMERIC '0') = coalesce(ms.reason_id, NUMERIC '0')
     AND coalesce(mt.reason_id, NUMERIC '1') = coalesce(ms.reason_id, NUMERIC '1'))
AND (coalesce(mt.calc_id, NUMERIC '0') = coalesce(ms.calc_id, NUMERIC '0')
     AND coalesce(mt.calc_id, NUMERIC '1') = coalesce(ms.calc_id, NUMERIC '1'))
AND (coalesce(mt.account_activity_id, NUMERIC '0') = coalesce(ms.account_activity_id, NUMERIC '0')
     AND coalesce(mt.account_activity_id, NUMERIC '1') = coalesce(ms.account_activity_id, NUMERIC '1'))
AND (coalesce(mt.account_payer_status_id, NUMERIC '0') = coalesce(ms.account_payer_status_id, NUMERIC '0')
     AND coalesce(mt.account_payer_status_id, NUMERIC '1') = coalesce(ms.account_payer_status_id, NUMERIC '1'))
AND (coalesce(mt.status_phase_id, NUMERIC '0') = coalesce(ms.status_phase_id, NUMERIC '0')
     AND coalesce(mt.status_phase_id, NUMERIC '1') = coalesce(ms.status_phase_id, NUMERIC '1'))
AND (coalesce(mt.status_category_id, NUMERIC '0') = coalesce(ms.status_category_id, NUMERIC '0')
     AND coalesce(mt.status_category_id, NUMERIC '1') = coalesce(ms.status_category_id, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        unit_num,
        pat_acct_num,
        iplan_id,
        ar_bill_thru_date,
        work_date,
        cc_drg_code,
        eor_hipps_code,
        length_of_stay_days_num,
        eor_payment_amt,
        eor_contractual_allowance_amt,
        eor_covered_days_num,
        eor_deductible_coinsurance_amt,
        eor_gross_reimbursement_amt,
        eor_sqstrtn_red_amt,
        eor_covered_charge_amt,
        eor_non_covered_charge_amt,
        eor_total_charge_amt,
        eor_insurance_payment_amt,
        eor_blood_deductible_amt,
        eor_capital_payment_amt,
        eor_coinsurance_amt,
        eor_deductible_amt,
        eor_total_actual_pat_resp_amt,
        eor_lab_payment_amt,
        eor_net_billed_charge_amt,
        eor_outlier_payment_amt,
        eor_primary_payor_payment_amt,
        eor_therapy_payment_amt,
        eor_copay_amt,
        pa_total_account_balance_amt,
        eor_net_apc_service_amt,
        eor_net_fee_schedule_amt,
        eor_total_variance_adj_amt,
        eor_total_trans_payment_amt,
        eor_hac_adj_amt,
        total_denial_amt,
        inpatient_outpatient_ind,
        interim_bill_ind,
        reason_code_3,
        reason_id,
        calc_id,
        account_activity_id,
        account_payer_status_id,
        status_phase_id,
        status_category_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.ar_bill_thru_date, ms.activity_due_date, ms.cc_drg_code, ms.eor_hipps_code, ms.length_of_stay_days_num, ms.eor_payment_amt, ms.eor_contractual_allowance_amt, ms.eor_covered_days_num, ms.eor_deductible_coinsurance_amt, ms.eor_gross_reimbursement_amt, ms.eor_sqstrtn_red_amt, ms.eor_covered_charge_amt, ms.eor_non_covered_charge_amt, ms.eor_total_charge_amt, ms.eor_insurance_payment_amt, ms.eor_blood_deductible_amt, ms.eor_capital_payment_amt, ms.eor_coinsurance_amt, ms.eor_deductible_amt, ms.eor_total_actual_pat_resp_amt, ms.eor_lab_payment_amt, ms.eor_net_billed_charge_amt, ms.eor_outlier_payment_amt, ms.eor_primary_payor_payment_amt, ms.eor_therapy_payment_amt, ms.eor_copay_amt, ms.pa_total_account_balance_amt, ms.eor_net_apc_service_amt, ms.eor_net_fee_schedule_amt, ms.eor_total_variance_adj_amt, ms.eor_total_trans_payment_amt, ms.eor_hac_adj_amt, ms.total_denial_amt, ms.inpatient_outpatient_ind, ms.interim_bill_ind, ms.reason_code_3, ms.reason_id, ms.calc_id, ms.account_activity_id, ms.account_payer_status_id, ms.status_phase_id, ms.status_category_id, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             log_id,
             log_sequence_num,
             eff_from_date
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.eor_discrepancy_wrk1');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_staging','EOR_Discrepancy_Wrk1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

RETURN;