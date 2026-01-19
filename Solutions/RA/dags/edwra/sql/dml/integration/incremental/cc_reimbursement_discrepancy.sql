DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reimbursement_discrepancy.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************************************************************
  Developer: Ray Holly
       Date: 05/23/2012
       Name: CC_Reimbursement_Discrepancy.sql
       Mod1: Fixed update statement on Reason Id, Account_Payer_Status_Id
             07/31/2012 FY.
	   Mod2: Fixed update on Var_Insurance_Payment_Amt, it was mapped to var_blood_deductable.
       Mod3: Added QUERY_BAND per Teradata DBA suggestion to meet standards. 11/7/2017 SW.
	   Mod4: Changed delete to only consider active coids on 1/30/2018 SW.
	Mod5:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
****************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA234;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy AS x USING
  (SELECT reimbursement_discrepancy_wrk1.company_code,
          reimbursement_discrepancy_wrk1.coid,
          reimbursement_discrepancy_wrk1.patient_dw_id,
          reimbursement_discrepancy_wrk1.payor_dw_id,
          reimbursement_discrepancy_wrk1.iplan_insurance_order_num,
          reimbursement_discrepancy_wrk1.eor_log_date,
          reimbursement_discrepancy_wrk1.log_id,
          reimbursement_discrepancy_wrk1.log_sequence_num,
          reimbursement_discrepancy_wrk1.eff_from_date,
          reimbursement_discrepancy_wrk1.unit_num,
          reimbursement_discrepancy_wrk1.pat_acct_num,
          reimbursement_discrepancy_wrk1.iplan_id,
          reimbursement_discrepancy_wrk1.length_of_stay_days_num,
          reimbursement_discrepancy_wrk1.eor_drg_code,
          reimbursement_discrepancy_wrk1.eor_hipps_code,
          reimbursement_discrepancy_wrk1.eor_payment_amt,
          reimbursement_discrepancy_wrk1.ar_bill_thru_date,
          reimbursement_discrepancy_wrk1.remittance_date,
          reimbursement_discrepancy_wrk1.work_date,
          reimbursement_discrepancy_wrk1.ra_log_date,
          reimbursement_discrepancy_wrk1.ra_payment_date,
          reimbursement_discrepancy_wrk1.ra_drg_code,
          reimbursement_discrepancy_wrk1.ra_hipps_code,
          reimbursement_discrepancy_wrk1.ra_covered_days_num,
          reimbursement_discrepancy_wrk1.ra_total_charge_amt,
          reimbursement_discrepancy_wrk1.ra_payment_amt,
          reimbursement_discrepancy_wrk1.ra_copay_amt,
          reimbursement_discrepancy_wrk1.ra_coinsurance_amt,
          reimbursement_discrepancy_wrk1.ra_contractual_allowance_amt,
          reimbursement_discrepancy_wrk1.ra_net_billed_charge_amt,
          reimbursement_discrepancy_wrk1.ra_gross_reimbursement_amt,
          reimbursement_discrepancy_wrk1.ra_non_covered_charge_amt,
          reimbursement_discrepancy_wrk1.ra_deductible_coinsurance_amt,
          reimbursement_discrepancy_wrk1.ra_deductible_amt,
          reimbursement_discrepancy_wrk1.ra_total_deductible_amt,
          reimbursement_discrepancy_wrk1.ra_blood_deductible_amt,
          reimbursement_discrepancy_wrk1.ra_lab_payment_amt,
          reimbursement_discrepancy_wrk1.ra_therapy_payment_amt,
          reimbursement_discrepancy_wrk1.ra_primary_payor_payment_amt,
          reimbursement_discrepancy_wrk1.ra_outlier_payment_amt,
          reimbursement_discrepancy_wrk1.ra_capital_payment_amt,
          reimbursement_discrepancy_wrk1.ra_denied_charge_amt,
          reimbursement_discrepancy_wrk1.ra_net_apc_service_amt,
          reimbursement_discrepancy_wrk1.ra_net_fee_schedule_amt,
          reimbursement_discrepancy_wrk1.discrepancy_origination_date,
          reimbursement_discrepancy_wrk1.actual_payment_amt,
          reimbursement_discrepancy_wrk1.pa_total_account_balance_amt,
          reimbursement_discrepancy_wrk1.over_under_payment_amt,
          reimbursement_discrepancy_wrk1.discrepancy_day_cnt,
          reimbursement_discrepancy_wrk1.var_covered_days_num,
          reimbursement_discrepancy_wrk1.var_copay_amt,
          reimbursement_discrepancy_wrk1.var_total_charge_amt,
          reimbursement_discrepancy_wrk1.var_covered_charge_amt,
          reimbursement_discrepancy_wrk1.var_non_covered_charge_amt,
          reimbursement_discrepancy_wrk1.var_net_billed_charge_amt,
          reimbursement_discrepancy_wrk1.var_gross_reimbursement_amt,
          reimbursement_discrepancy_wrk1.var_deductible_amt,
          reimbursement_discrepancy_wrk1.var_coinsurance_amt,
          reimbursement_discrepancy_wrk1.var_deductible_coinsurance_amt,
          reimbursement_discrepancy_wrk1.var_blood_deductible_amt,
          reimbursement_discrepancy_wrk1.var_insurance_payment_amt,
          reimbursement_discrepancy_wrk1.var_primary_payor_payment_amt,
          reimbursement_discrepancy_wrk1.var_capital_payment_amt,
          reimbursement_discrepancy_wrk1.var_lab_payment_amt,
          reimbursement_discrepancy_wrk1.var_outlier_payment_amt,
          reimbursement_discrepancy_wrk1.var_therapy_payment_amt,
          reimbursement_discrepancy_wrk1.var_contractual_allowance_amt,
          reimbursement_discrepancy_wrk1.var_net_apc_service_amt,
          reimbursement_discrepancy_wrk1.var_net_fee_schedule_amt,
          reimbursement_discrepancy_wrk1.inpatient_outpatient_ind,
          reimbursement_discrepancy_wrk1.reason_code_3,
          reimbursement_discrepancy_wrk1.calc_id,
          reimbursement_discrepancy_wrk1.account_activity_id,
          reimbursement_discrepancy_wrk1.reason_id,
          reimbursement_discrepancy_wrk1.account_payer_status_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_discrepancy_wrk1) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num WHEN MATCHED THEN
UPDATE
SET length_of_stay_days_num = z.length_of_stay_days_num,
    eor_drg_code = z.eor_drg_code,
    eor_hipps_code = z.eor_hipps_code,
    eor_payment_amt = z.eor_payment_amt,
    ar_bill_thru_date = z.ar_bill_thru_date,
    remittance_date = z.remittance_date,
    work_date = z.work_date,
    ra_log_date = z.ra_log_date,
    ra_payment_date = z.ra_payment_date,
    ra_drg_code = z.ra_drg_code,
    ra_hipps_code = z.ra_hipps_code,
    ra_covered_days_num = z.ra_covered_days_num,
    ra_total_charge_amt = z.ra_total_charge_amt,
    ra_payment_amt = z.ra_payment_amt,
    ra_copay_amt = z.ra_copay_amt,
    ra_coinsurance_amt = z.ra_coinsurance_amt,
    ra_contractual_allowance_amt = z.ra_contractual_allowance_amt,
    ra_net_billed_charge_amt = z.ra_net_billed_charge_amt,
    ra_gross_reimbursement_amt = z.ra_gross_reimbursement_amt,
    ra_non_covered_charge_amt = z.ra_non_covered_charge_amt,
    ra_deductible_coinsurance_amt = z.ra_deductible_coinsurance_amt,
    ra_deductible_amt = z.ra_deductible_amt,
    ra_total_deductible_amt = z.ra_total_deductible_amt,
    ra_blood_deductible_amt = z.ra_blood_deductible_amt,
    ra_lab_payment_amt = z.ra_lab_payment_amt,
    ra_therapy_payment_amt = z.ra_therapy_payment_amt,
    ra_primary_payor_payment_amt = z.ra_primary_payor_payment_amt,
    ra_outlier_payment_amt = z.ra_outlier_payment_amt,
    ra_capital_payment_amt = z.ra_capital_payment_amt,
    ra_denied_charge_amt = z.ra_denied_charge_amt,
    ra_net_apc_service_amt = z.ra_net_apc_service_amt,
    ra_net_fee_schedule_amt = z.ra_net_fee_schedule_amt,
    actual_payment_amt = z.actual_payment_amt,
    pa_total_account_balance_amt = z.pa_total_account_balance_amt,
    over_under_payment_amt = z.over_under_payment_amt,
    discrepancy_day_cnt = date_diff(current_date('US/Central'), x.discrepancy_origination_date, DAY),
    var_covered_days_num = z.var_covered_days_num,
    var_copay_amt = z.var_copay_amt,
    var_total_charge_amt = z.var_total_charge_amt,
    var_covered_charge_amt = z.var_covered_charge_amt,
    var_non_covered_charge_amt = z.var_non_covered_charge_amt,
    var_net_billed_charge_amt = z.var_net_billed_charge_amt,
    var_gross_reimbursement_amt = z.var_gross_reimbursement_amt,
    var_deductible_amt = z.var_deductible_amt,
    var_coinsurance_amt = z.var_coinsurance_amt,
    var_deductible_coinsurance_amt = z.var_deductible_coinsurance_amt,
    var_blood_deductible_amt = z.var_blood_deductible_amt,
    var_insurance_payment_amt = z.var_insurance_payment_amt,
    var_primary_payor_payment_amt = z.var_primary_payor_payment_amt,
    var_capital_payment_amt = z.var_capital_payment_amt,
    var_lab_payment_amt = z.var_lab_payment_amt,
    var_outlier_payment_amt = z.var_outlier_payment_amt,
    var_therapy_payment_amt = z.var_therapy_payment_amt,
    var_contractual_allowance_amt = z.var_contractual_allowance_amt,
    var_net_apc_service_amt = z.var_net_apc_service_amt,
    var_net_fee_schedule_amt = z.var_net_fee_schedule_amt,
    inpatient_outpatient_ind = z.inpatient_outpatient_ind,
    reason_code_3 = z.reason_code_3,
    account_activity_id = z.account_activity_id,
    reason_id = z.reason_id,
    account_payer_status_id = z.account_payer_status_id,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
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
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.iplan_insurance_order_num, z.eor_log_date, z.log_id, z.log_sequence_num, z.eff_from_date, z.unit_num, z.pat_acct_num, z.iplan_id, z.length_of_stay_days_num, z.eor_drg_code, z.eor_hipps_code, z.eor_payment_amt, z.ar_bill_thru_date, z.remittance_date, z.work_date, z.ra_log_date, z.ra_payment_date, z.ra_drg_code, z.ra_hipps_code, z.ra_covered_days_num, z.ra_total_charge_amt, z.ra_payment_amt, z.ra_copay_amt, z.ra_coinsurance_amt, z.ra_contractual_allowance_amt, z.ra_net_billed_charge_amt, z.ra_gross_reimbursement_amt, z.ra_non_covered_charge_amt, z.ra_deductible_coinsurance_amt, z.ra_deductible_amt, z.ra_total_deductible_amt, z.ra_blood_deductible_amt, z.ra_lab_payment_amt, z.ra_therapy_payment_amt, z.ra_primary_payor_payment_amt, z.ra_outlier_payment_amt, z.ra_capital_payment_amt, z.ra_denied_charge_amt, z.ra_net_apc_service_amt, z.ra_net_fee_schedule_amt, z.discrepancy_origination_date, z.actual_payment_amt, z.pa_total_account_balance_amt, z.over_under_payment_amt, z.discrepancy_day_cnt, z.var_covered_days_num, z.var_copay_amt, z.var_total_charge_amt, z.var_covered_charge_amt, z.var_non_covered_charge_amt, z.var_net_billed_charge_amt, z.var_gross_reimbursement_amt, z.var_deductible_amt, z.var_coinsurance_amt, z.var_deductible_coinsurance_amt, z.var_blood_deductible_amt, z.var_insurance_payment_amt, z.var_primary_payor_payment_amt, z.var_capital_payment_amt, z.var_lab_payment_amt, z.var_outlier_payment_amt, z.var_therapy_payment_amt, z.var_contractual_allowance_amt, z.var_net_apc_service_amt, z.var_net_fee_schedule_amt, z.inpatient_outpatient_ind, z.reason_code_3, z.calc_id, z.account_activity_id, z.reason_id, z.account_payer_status_id, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


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
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy
WHERE upper(cc_reimbursement_discrepancy.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_reimbursement_discrepancy.dw_last_update_date_time <>
    (SELECT max(cc_reimbursement_discrepancy_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimbursement_discrepancy AS cc_reimbursement_discrepancy_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Reimbursement_Discrepancy');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

RETURN;