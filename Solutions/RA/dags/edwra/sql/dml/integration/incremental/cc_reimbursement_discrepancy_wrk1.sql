DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reimbursement_discrepancy_wrk1.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************************************************************
  Developer: Ray Holly
       Date: 05/23/2012
       Name: CC_Reimbursement_Discrepancy_Wrk1.sql
       Mod1: Updated Over_Under_Payment Definition
             07/31/2012 FY.
       Mod2: Updated Over_Under_Payment with sequest amt
             03/20/2014 FY.
       Mod3: Added Reason_Code_3  07/21/2014 MFY.
       Mod4: Changing mapping on Over_Under_Payment_Amt & Var_Gross_Reimbursement_Amt
             to use mon_account_payer total patient responsibility. 1/29/2015 AP.
       Mod5: Changed the Discrepancy_Origination_Date logic to be yesterdays date rather than current date. 02/12/2015. AP.
       Mod6: Changing mapping on Over_Under_Payment_Amt & Var_Gross_Reimbursement_Amt to exclude Hac amount. 06/22/2015. AP.
	   Mod7: Added QUERY_BAND per Teradata DBA suggestion to meet standards. 11/7/2017 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
****************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA234_Wrk1;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_discrepancy_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_discrepancy_wrk1 AS mt USING
  (SELECT DISTINCT e1.company_code AS company_code,
                   e1.coid AS coid,
                   e1.patient_dw_id,
                   e1.payor_dw_id,
                   e1.iplan_insurance_order_num,
                   e1.eor_log_date,
                   e1.log_id AS log_id,
                   e1.log_sequence_num,
                   e1.eff_from_date,
                   e1.unit_num AS unit_num,
                   e1.pat_acct_num,
                   e1.iplan_id,
                   e1.length_of_stay_days_num,
                   e1.cc_drg_code AS eor_drg_code,
                   e1.eor_hipps_code AS eor_hipps_code,
                   e1.eor_payment_amt,
                   coalesce(e1.ar_bill_thru_date, DATE '1800-01-01') AS ar_bill_thru_date,
                   r1.remittance_date,
                   e1.work_date,
                   r1.ra_log_date,
                   r1.ra_payment_date,
                   r1.ra_drg_code AS ra_drg_code,
                   r1.ra_hipps_code AS ra_hipps_code,
                   r1.ra_covered_days_num,
                   r1.ra_total_charge_amt,
                   r1.ra_payment_amt,
                   r1.ra_copay_amt,
                   r1.ra_coinsurance_amt,
                   r1.ra_contractual_allowance_amt,
                   r1.ra_net_billed_charge_amt,
                   r1.ra_gross_reimbursement_amt,
                   r1.ra_non_covered_charge_amt,
                   r1.ra_deductible_coinsurance_amt,
                   r1.ra_deductible_amt,
                   r1.ra_total_deductible_amt,
                   r1.ra_blood_deductible_amt,
                   r1.ra_lab_payment_amt,
                   r1.ra_therapy_payment_amt,
                   r1.ra_primary_payor_payment_amt,
                   r1.ra_outlier_payment_amt,
                   r1.ra_capital_payment_amt,
                   r1.ra_denied_charge_amt,
                   r1.ra_net_apc_service_amt,
                   r1.ra_net_fee_schedule_amt,
                   DATE(datetime_trunc(current_datetime('US/Central'), SECOND) - INTERVAL 1 DAY) AS discrepancy_origination_date,
                   r1.actual_payment_amt,
                   e1.pa_total_account_balance_amt,
                   CAST(ROUND(coalesce(e1.eor_gross_reimbursement_amt, NUMERIC '0') - coalesce(e1.eor_total_trans_payment_amt, NUMERIC '0') - coalesce(e1.eor_total_actual_pat_resp_amt, NUMERIC '0') - coalesce(e1.eor_total_variance_adj_amt, NUMERIC '0') - coalesce(e1.total_denial_amt, NUMERIC '0') - coalesce(e1.eor_sqstrtn_red_amt, NUMERIC '0') - coalesce(e1.eor_hac_adj_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS over_under_payment_amt,
                   0 AS discrepancy_day_cnt,
                   var_covered_days_num AS var_covered_days_num,
                   var_copay_amt AS var_copay_amt,
                   var_total_charge_amt AS var_total_charge_amt,
                   var_covered_charge_amt AS var_covered_charge_amt,
                   var_non_covered_charge_amt AS var_non_covered_charge_amt,
                   var_net_billed_charge_amt AS var_net_billed_charge_amt,
                   var_gross_reimbursement_amt AS var_gross_reimbursement_amt,
                   var_deductible_amt AS var_deductible_amt,
                   var_coinsurance_amt AS var_coinsurance_amt,
                   var_deductible_coinsurance_amt AS var_deductible_coinsurance_amt,
                   var_blood_deductible_amt AS var_blood_deductible_amt,
                   var_insurance_payment_amt AS var_insurance_payment_amt,
                   var_primary_payor_payment_amt AS var_primary_payor_payment_amt,
                   var_capital_payment_amt AS var_capital_payment_amt,
                   var_lab_payment_amt AS var_lab_payment_amt,
                   var_outlier_payment_amt AS var_outlier_payment_amt,
                   var_therapy_payment_amt AS var_therapy_payment_amt,
                   var_contractual_allowance_amt AS var_contractual_allowance_amt,
                   var_net_apc_service_amt AS var_net_apc_service_amt,
                   var_net_fee_schedule_amt AS var_net_fee_schedule_amt,
                   e1.inpatient_outpatient_ind AS inpatient_outpatient_ind,
                   e1.reason_code_3 AS reason_code_3,
                   e1.calc_id,
                   e1.account_activity_id,
                   e1.reason_id,
                   e1.account_payer_status_id,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS e1
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_discrepancy_wrk1 AS r1 ON upper(rtrim(e1.company_code)) = upper(rtrim(r1.company_code))
   AND upper(rtrim(e1.coid)) = upper(rtrim(r1.coid))
   AND e1.patient_dw_id = r1.patient_dw_id
   AND e1.payor_dw_id = r1.payor_dw_id
   AND e1.iplan_insurance_order_num = r1.iplan_insurance_order_num
   AND e1.eor_log_date = r1.eor_log_date
   AND upper(rtrim(e1.log_id)) = upper(rtrim(r1.log_id))
   AND e1.log_sequence_num = r1.log_sequence_num
   AND e1.eff_from_date = r1.eff_from_date
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_net_fee_schedule_amt, NUMERIC '0') - coalesce(r1.ra_net_fee_schedule_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_net_fee_schedule_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_net_apc_service_amt, NUMERIC '0') - coalesce(r1.ra_net_apc_service_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_net_apc_service_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_contractual_allowance_amt, NUMERIC '0') - coalesce(r1.ra_contractual_allowance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_contractual_allowance_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_therapy_payment_amt, NUMERIC '0') - coalesce(r1.ra_therapy_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_therapy_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_outlier_payment_amt, NUMERIC '0') - coalesce(r1.ra_outlier_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_outlier_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_lab_payment_amt, NUMERIC '0') - coalesce(r1.ra_lab_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_lab_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_capital_payment_amt, NUMERIC '0') - coalesce(r1.ra_capital_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_capital_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_primary_payor_payment_amt, NUMERIC '0') - coalesce(r1.ra_primary_payor_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_primary_payor_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_insurance_payment_amt, NUMERIC '0') - coalesce(r1.ra_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_insurance_payment_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_blood_deductible_amt, NUMERIC '0') - coalesce(r1.ra_blood_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_blood_deductible_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_deductible_coinsurance_amt, NUMERIC '0') - coalesce(r1.ra_deductible_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_deductible_coinsurance_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_coinsurance_amt, NUMERIC '0') - coalesce(r1.ra_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_coinsurance_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_deductible_amt, NUMERIC '0') - coalesce(r1.ra_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_deductible_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_gross_reimbursement_amt, NUMERIC '0') - coalesce(e1.eor_total_trans_payment_amt, NUMERIC '0') - coalesce(e1.eor_total_actual_pat_resp_amt, NUMERIC '0') - coalesce(e1.eor_total_variance_adj_amt, NUMERIC '0') - coalesce(e1.total_denial_amt, NUMERIC '0') - coalesce(e1.eor_sqstrtn_red_amt, NUMERIC '0') - coalesce(e1.eor_hac_adj_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_gross_reimbursement_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_net_billed_charge_amt, NUMERIC '0') - coalesce(r1.ra_net_billed_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_net_billed_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_non_covered_charge_amt, NUMERIC '0') - coalesce(r1.ra_non_covered_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_non_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_covered_charge_amt, NUMERIC '0') - (coalesce(r1.ra_total_charge_amt, NUMERIC '0') + coalesce(r1.ra_non_covered_charge_amt, NUMERIC '0')), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_total_charge_amt, NUMERIC '0') - coalesce(r1.ra_total_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_total_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(e1.eor_copay_amt, NUMERIC '0') - coalesce(r1.ra_copay_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS var_copay_amt
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN upper(rtrim(e1.inpatient_outpatient_ind)) = 'O' THEN 0
                                ELSE coalesce(e1.eor_covered_days_num, 0) - coalesce(r1.ra_covered_days_num, 0)
                            END ]) AS var_covered_days_num
   WHERE var_covered_days_num <> 0
     OR var_copay_amt <> 0
     OR var_total_charge_amt <> 0
     OR var_covered_charge_amt <> 0
     OR var_non_covered_charge_amt <> 0
     OR var_net_billed_charge_amt <> 0
     OR var_gross_reimbursement_amt <> 0
     OR var_deductible_amt <> 0
     OR var_coinsurance_amt <> 0
     OR var_deductible_coinsurance_amt <> 0
     OR var_blood_deductible_amt <> 0
     OR var_insurance_payment_amt <> 0
     OR var_primary_payor_payment_amt <> 0
     OR var_capital_payment_amt <> 0
     OR var_lab_payment_amt <> 0
     OR var_outlier_payment_amt <> 0
     OR var_therapy_payment_amt <> 0
     OR var_contractual_allowance_amt <> 0
     OR var_net_apc_service_amt <> 0
     OR var_net_fee_schedule_amt <> 0
     OR upper(rtrim(coalesce(e1.cc_drg_code, '0'))) <> upper(rtrim(coalesce(r1.ra_drg_code, '0')))
     OR upper(rtrim(coalesce(e1.eor_hipps_code, '0'))) <> upper(rtrim(coalesce(r1.ra_hipps_code, '0'))) ) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
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
AND (coalesce(mt.length_of_stay_days_num, 0) = coalesce(ms.length_of_stay_days_num, 0)
     AND coalesce(mt.length_of_stay_days_num, 1) = coalesce(ms.length_of_stay_days_num, 1))
AND (upper(coalesce(mt.eor_drg_code, '0')) = upper(coalesce(ms.eor_drg_code, '0'))
     AND upper(coalesce(mt.eor_drg_code, '1')) = upper(coalesce(ms.eor_drg_code, '1')))
AND (upper(coalesce(mt.eor_hipps_code, '0')) = upper(coalesce(ms.eor_hipps_code, '0'))
     AND upper(coalesce(mt.eor_hipps_code, '1')) = upper(coalesce(ms.eor_hipps_code, '1')))
AND (coalesce(mt.eor_payment_amt, NUMERIC '0') = coalesce(ms.eor_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_payment_amt, NUMERIC '1') = coalesce(ms.eor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (coalesce(mt.remittance_date, DATE '1970-01-01') = coalesce(ms.remittance_date, DATE '1970-01-01')
     AND coalesce(mt.remittance_date, DATE '1970-01-02') = coalesce(ms.remittance_date, DATE '1970-01-02'))
AND (coalesce(mt.work_date, DATE '1970-01-01') = coalesce(ms.work_date, DATE '1970-01-01')
     AND coalesce(mt.work_date, DATE '1970-01-02') = coalesce(ms.work_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_payment_date, DATE '1970-01-01') = coalesce(ms.ra_payment_date, DATE '1970-01-01')
     AND coalesce(mt.ra_payment_date, DATE '1970-01-02') = coalesce(ms.ra_payment_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.ra_drg_code, '0')) = upper(coalesce(ms.ra_drg_code, '0'))
     AND upper(coalesce(mt.ra_drg_code, '1')) = upper(coalesce(ms.ra_drg_code, '1')))
AND (upper(coalesce(mt.ra_hipps_code, '0')) = upper(coalesce(ms.ra_hipps_code, '0'))
     AND upper(coalesce(mt.ra_hipps_code, '1')) = upper(coalesce(ms.ra_hipps_code, '1')))
AND (coalesce(mt.ra_covered_days_num, 0) = coalesce(ms.ra_covered_days_num, 0)
     AND coalesce(mt.ra_covered_days_num, 1) = coalesce(ms.ra_covered_days_num, 1))
AND (coalesce(mt.ra_total_charge_amt, NUMERIC '0') = coalesce(ms.ra_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_charge_amt, NUMERIC '1') = coalesce(ms.ra_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_payment_amt, NUMERIC '0') = coalesce(ms.ra_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_payment_amt, NUMERIC '1') = coalesce(ms.ra_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_copay_amt, NUMERIC '0') = coalesce(ms.ra_copay_amt, NUMERIC '0')
     AND coalesce(mt.ra_copay_amt, NUMERIC '1') = coalesce(ms.ra_copay_amt, NUMERIC '1'))
AND (coalesce(mt.ra_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_contractual_allowance_amt, NUMERIC '0') = coalesce(ms.ra_contractual_allowance_amt, NUMERIC '0')
     AND coalesce(mt.ra_contractual_allowance_amt, NUMERIC '1') = coalesce(ms.ra_contractual_allowance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_billed_charge_amt, NUMERIC '0') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_billed_charge_amt, NUMERIC '1') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_gross_reimbursement_amt, NUMERIC '0') = coalesce(ms.ra_gross_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.ra_gross_reimbursement_amt, NUMERIC '1') = coalesce(ms.ra_gross_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.ra_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_deductible_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_deductible_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_deductible_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_deductible_amt, NUMERIC '0') = coalesce(ms.ra_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_amt, NUMERIC '1') = coalesce(ms.ra_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_total_deductible_amt, NUMERIC '0') = coalesce(ms.ra_total_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_deductible_amt, NUMERIC '1') = coalesce(ms.ra_total_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_blood_deductible_amt, NUMERIC '0') = coalesce(ms.ra_blood_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_blood_deductible_amt, NUMERIC '1') = coalesce(ms.ra_blood_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_lab_payment_amt, NUMERIC '0') = coalesce(ms.ra_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_lab_payment_amt, NUMERIC '1') = coalesce(ms.ra_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_therapy_payment_amt, NUMERIC '0') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_therapy_payment_amt, NUMERIC '1') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_primary_payor_payment_amt, NUMERIC '0') = coalesce(ms.ra_primary_payor_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_primary_payor_payment_amt, NUMERIC '1') = coalesce(ms.ra_primary_payor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_outlier_payment_amt, NUMERIC '0') = coalesce(ms.ra_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_outlier_payment_amt, NUMERIC '1') = coalesce(ms.ra_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_capital_payment_amt, NUMERIC '0') = coalesce(ms.ra_capital_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_capital_payment_amt, NUMERIC '1') = coalesce(ms.ra_capital_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_denied_charge_amt, NUMERIC '0') = coalesce(ms.ra_denied_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_denied_charge_amt, NUMERIC '1') = coalesce(ms.ra_denied_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_apc_service_amt, NUMERIC '0') = coalesce(ms.ra_net_apc_service_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_apc_service_amt, NUMERIC '1') = coalesce(ms.ra_net_apc_service_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_fee_schedule_amt, NUMERIC '0') = coalesce(ms.ra_net_fee_schedule_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_fee_schedule_amt, NUMERIC '1') = coalesce(ms.ra_net_fee_schedule_amt, NUMERIC '1'))
AND (coalesce(mt.discrepancy_origination_date, DATE '1970-01-01') = coalesce(ms.discrepancy_origination_date, DATE '1970-01-01')
     AND coalesce(mt.discrepancy_origination_date, DATE '1970-01-02') = coalesce(ms.discrepancy_origination_date, DATE '1970-01-02'))
AND (coalesce(mt.actual_payment_amt, NUMERIC '0') = coalesce(ms.actual_payment_amt, NUMERIC '0')
     AND coalesce(mt.actual_payment_amt, NUMERIC '1') = coalesce(ms.actual_payment_amt, NUMERIC '1'))
AND (coalesce(mt.pa_total_account_balance_amt, NUMERIC '0') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '0')
     AND coalesce(mt.pa_total_account_balance_amt, NUMERIC '1') = coalesce(ms.pa_total_account_balance_amt, NUMERIC '1'))
AND (coalesce(mt.over_under_payment_amt, NUMERIC '0') = coalesce(ms.over_under_payment_amt, NUMERIC '0')
     AND coalesce(mt.over_under_payment_amt, NUMERIC '1') = coalesce(ms.over_under_payment_amt, NUMERIC '1'))
AND (coalesce(mt.discrepancy_day_cnt, 0) = coalesce(ms.discrepancy_day_cnt, 0)
     AND coalesce(mt.discrepancy_day_cnt, 1) = coalesce(ms.discrepancy_day_cnt, 1))
AND (coalesce(mt.var_covered_days_num, 0) = coalesce(ms.var_covered_days_num, 0)
     AND coalesce(mt.var_covered_days_num, 1) = coalesce(ms.var_covered_days_num, 1))
AND (coalesce(mt.var_copay_amt, NUMERIC '0') = coalesce(ms.var_copay_amt, NUMERIC '0')
     AND coalesce(mt.var_copay_amt, NUMERIC '1') = coalesce(ms.var_copay_amt, NUMERIC '1'))
AND (coalesce(mt.var_total_charge_amt, NUMERIC '0') = coalesce(ms.var_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.var_total_charge_amt, NUMERIC '1') = coalesce(ms.var_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.var_covered_charge_amt, NUMERIC '0') = coalesce(ms.var_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.var_covered_charge_amt, NUMERIC '1') = coalesce(ms.var_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.var_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.var_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.var_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.var_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.var_net_billed_charge_amt, NUMERIC '0') = coalesce(ms.var_net_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.var_net_billed_charge_amt, NUMERIC '1') = coalesce(ms.var_net_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.var_gross_reimbursement_amt, NUMERIC '0') = coalesce(ms.var_gross_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.var_gross_reimbursement_amt, NUMERIC '1') = coalesce(ms.var_gross_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.var_deductible_amt, NUMERIC '0') = coalesce(ms.var_deductible_amt, NUMERIC '0')
     AND coalesce(mt.var_deductible_amt, NUMERIC '1') = coalesce(ms.var_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.var_coinsurance_amt, NUMERIC '0') = coalesce(ms.var_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.var_coinsurance_amt, NUMERIC '1') = coalesce(ms.var_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.var_deductible_coinsurance_amt, NUMERIC '0') = coalesce(ms.var_deductible_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.var_deductible_coinsurance_amt, NUMERIC '1') = coalesce(ms.var_deductible_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.var_blood_deductible_amt, NUMERIC '0') = coalesce(ms.var_blood_deductible_amt, NUMERIC '0')
     AND coalesce(mt.var_blood_deductible_amt, NUMERIC '1') = coalesce(ms.var_blood_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.var_insurance_payment_amt, NUMERIC '0') = coalesce(ms.var_insurance_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_insurance_payment_amt, NUMERIC '1') = coalesce(ms.var_insurance_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_primary_payor_payment_amt, NUMERIC '0') = coalesce(ms.var_primary_payor_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_primary_payor_payment_amt, NUMERIC '1') = coalesce(ms.var_primary_payor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_capital_payment_amt, NUMERIC '0') = coalesce(ms.var_capital_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_capital_payment_amt, NUMERIC '1') = coalesce(ms.var_capital_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_lab_payment_amt, NUMERIC '0') = coalesce(ms.var_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_lab_payment_amt, NUMERIC '1') = coalesce(ms.var_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_outlier_payment_amt, NUMERIC '0') = coalesce(ms.var_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_outlier_payment_amt, NUMERIC '1') = coalesce(ms.var_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_therapy_payment_amt, NUMERIC '0') = coalesce(ms.var_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_therapy_payment_amt, NUMERIC '1') = coalesce(ms.var_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_contractual_allowance_amt, NUMERIC '0') = coalesce(ms.var_contractual_allowance_amt, NUMERIC '0')
     AND coalesce(mt.var_contractual_allowance_amt, NUMERIC '1') = coalesce(ms.var_contractual_allowance_amt, NUMERIC '1'))
AND (coalesce(mt.var_net_apc_service_amt, NUMERIC '0') = coalesce(ms.var_net_apc_service_amt, NUMERIC '0')
     AND coalesce(mt.var_net_apc_service_amt, NUMERIC '1') = coalesce(ms.var_net_apc_service_amt, NUMERIC '1'))
AND (coalesce(mt.var_net_fee_schedule_amt, NUMERIC '0') = coalesce(ms.var_net_fee_schedule_amt, NUMERIC '0')
     AND coalesce(mt.var_net_fee_schedule_amt, NUMERIC '1') = coalesce(ms.var_net_fee_schedule_amt, NUMERIC '1'))
AND (upper(coalesce(mt.inpatient_outpatient_ind, '0')) = upper(coalesce(ms.inpatient_outpatient_ind, '0'))
     AND upper(coalesce(mt.inpatient_outpatient_ind, '1')) = upper(coalesce(ms.inpatient_outpatient_ind, '1')))
AND (upper(coalesce(mt.reason_code_3, '0')) = upper(coalesce(ms.reason_code_3, '0'))
     AND upper(coalesce(mt.reason_code_3, '1')) = upper(coalesce(ms.reason_code_3, '1')))
AND (coalesce(mt.calc_id, NUMERIC '0') = coalesce(ms.calc_id, NUMERIC '0')
     AND coalesce(mt.calc_id, NUMERIC '1') = coalesce(ms.calc_id, NUMERIC '1'))
AND (coalesce(mt.account_activity_id, NUMERIC '0') = coalesce(ms.account_activity_id, NUMERIC '0')
     AND coalesce(mt.account_activity_id, NUMERIC '1') = coalesce(ms.account_activity_id, NUMERIC '1'))
AND (coalesce(mt.reason_id, NUMERIC '0') = coalesce(ms.reason_id, NUMERIC '0')
     AND coalesce(mt.reason_id, NUMERIC '1') = coalesce(ms.reason_id, NUMERIC '1'))
AND (coalesce(mt.account_payer_status_id, NUMERIC '0') = coalesce(ms.account_payer_status_id, NUMERIC '0')
     AND coalesce(mt.account_payer_status_id, NUMERIC '1') = coalesce(ms.account_payer_status_id, NUMERIC '1'))
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
        length_of_stay_days_num,
        eor_drg_code,
        eor_hipps_code,
        eor_payment_amt,
        ar_bill_thru_date,
        remittance_date,
        work_date,
        ra_log_date,
        ra_payment_date,
        ra_drg_code,
        ra_hipps_code,
        ra_covered_days_num,
        ra_total_charge_amt,
        ra_payment_amt,
        ra_copay_amt,
        ra_coinsurance_amt,
        ra_contractual_allowance_amt,
        ra_net_billed_charge_amt,
        ra_gross_reimbursement_amt,
        ra_non_covered_charge_amt,
        ra_deductible_coinsurance_amt,
        ra_deductible_amt,
        ra_total_deductible_amt,
        ra_blood_deductible_amt,
        ra_lab_payment_amt,
        ra_therapy_payment_amt,
        ra_primary_payor_payment_amt,
        ra_outlier_payment_amt,
        ra_capital_payment_amt,
        ra_denied_charge_amt,
        ra_net_apc_service_amt,
        ra_net_fee_schedule_amt,
        discrepancy_origination_date,
        actual_payment_amt,
        pa_total_account_balance_amt,
        over_under_payment_amt,
        discrepancy_day_cnt,
        var_covered_days_num,
        var_copay_amt,
        var_total_charge_amt,
        var_covered_charge_amt,
        var_non_covered_charge_amt,
        var_net_billed_charge_amt,
        var_gross_reimbursement_amt,
        var_deductible_amt,
        var_coinsurance_amt,
        var_deductible_coinsurance_amt,
        var_blood_deductible_amt,
        var_insurance_payment_amt,
        var_primary_payor_payment_amt,
        var_capital_payment_amt,
        var_lab_payment_amt,
        var_outlier_payment_amt,
        var_therapy_payment_amt,
        var_contractual_allowance_amt,
        var_net_apc_service_amt,
        var_net_fee_schedule_amt,
        inpatient_outpatient_ind,
        reason_code_3,
        calc_id,
        account_activity_id,
        reason_id,
        account_payer_status_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.length_of_stay_days_num, ms.eor_drg_code, ms.eor_hipps_code, ms.eor_payment_amt, ms.ar_bill_thru_date, ms.remittance_date, ms.work_date, ms.ra_log_date, ms.ra_payment_date, ms.ra_drg_code, ms.ra_hipps_code, ms.ra_covered_days_num, ms.ra_total_charge_amt, ms.ra_payment_amt, ms.ra_copay_amt, ms.ra_coinsurance_amt, ms.ra_contractual_allowance_amt, ms.ra_net_billed_charge_amt, ms.ra_gross_reimbursement_amt, ms.ra_non_covered_charge_amt, ms.ra_deductible_coinsurance_amt, ms.ra_deductible_amt, ms.ra_total_deductible_amt, ms.ra_blood_deductible_amt, ms.ra_lab_payment_amt, ms.ra_therapy_payment_amt, ms.ra_primary_payor_payment_amt, ms.ra_outlier_payment_amt, ms.ra_capital_payment_amt, ms.ra_denied_charge_amt, ms.ra_net_apc_service_amt, ms.ra_net_fee_schedule_amt, ms.discrepancy_origination_date, ms.actual_payment_amt, ms.pa_total_account_balance_amt, ms.over_under_payment_amt, ms.discrepancy_day_cnt, ms.var_covered_days_num, ms.var_copay_amt, ms.var_total_charge_amt, ms.var_covered_charge_amt, ms.var_non_covered_charge_amt, ms.var_net_billed_charge_amt, ms.var_gross_reimbursement_amt, ms.var_deductible_amt, ms.var_coinsurance_amt, ms.var_deductible_coinsurance_amt, ms.var_blood_deductible_amt, ms.var_insurance_payment_amt, ms.var_primary_payor_payment_amt, ms.var_capital_payment_amt, ms.var_lab_payment_amt, ms.var_outlier_payment_amt, ms.var_therapy_payment_amt, ms.var_contractual_allowance_amt, ms.var_net_apc_service_amt, ms.var_net_fee_schedule_amt, ms.inpatient_outpatient_ind, ms.reason_code_3, ms.calc_id, ms.account_activity_id, ms.reason_id, ms.account_payer_status_id, ms.dw_last_update_date_time, ms.source_system_code);


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
             log_sequence_num
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_discrepancy_wrk1
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.reimbursement_discrepancy_wrk1');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','Reimbursement_Discrepancy_Wrk1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

RETURN;