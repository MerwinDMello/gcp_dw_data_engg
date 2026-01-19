DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reimbursement_discrepancy_rd_parallel.sql
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
	Mod6: PBI 18545 - Updated this script specifically for a parallel run on 10/9/2018.  BC.
	Mod7: PBI 18545 - Updated this script so that process_date_from_etl_load.process_date is the max of ETL_LOAD_DATE instead of ETL_LOAD_REIMBURSEMENT_DISCREPANCY_DATE on 11/2/2018.  BC.
****************************************************************************************************************************/ -- SET QUERY_BAND = 'App=RA_Group2_ETL;Job=CTDRA234;' FOR SESSION;
 -- PBI 18545: Using the staging table that I created.
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimbursement_discrepancy_rd_parallel AS x USING -- PBI 18545: Inserting into a parallel version of CC_Reimbursement_Discrepancy

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
          a.length_of_stay_days_num,
          a.eor_drg_code,
          a.eor_hipps_code,
          a.eor_payment_amt,
          a.ar_bill_thru_date,
          a.remittance_date,
          a.work_date,
          a.ra_log_date,
          a.ra_payment_date,
          a.ra_drg_code,
          a.ra_hipps_code,
          a.ra_covered_days_num,
          a.ra_total_charge_amt,
          a.ra_payment_amt,
          a.ra_copay_amt,
          a.ra_coinsurance_amt,
          a.ra_contractual_allowance_amt,
          a.ra_net_billed_charge_amt,
          a.ra_gross_reimbursement_amt,
          a.ra_non_covered_charge_amt,
          a.ra_deductible_coinsurance_amt,
          a.ra_deductible_amt,
          a.ra_total_deductible_amt,
          a.ra_blood_deductible_amt,
          a.ra_lab_payment_amt,
          a.ra_therapy_payment_amt,
          a.ra_primary_payor_payment_amt,
          a.ra_outlier_payment_amt,
          a.ra_capital_payment_amt,
          a.ra_denied_charge_amt,
          a.ra_net_apc_service_amt,
          a.ra_net_fee_schedule_amt,
          a.discrepancy_origination_date,
          a.actual_payment_amt,
          a.pa_total_account_balance_amt,
          a.over_under_payment_amt,
          a.discrepancy_day_cnt,
          a.var_covered_days_num,
          a.var_copay_amt,
          a.var_total_charge_amt,
          a.var_covered_charge_amt,
          a.var_non_covered_charge_amt,
          a.var_net_billed_charge_amt,
          a.var_gross_reimbursement_amt,
          a.var_deductible_amt,
          a.var_coinsurance_amt,
          a.var_deductible_coinsurance_amt,
          a.var_blood_deductible_amt,
          a.var_insurance_payment_amt,
          a.var_primary_payor_payment_amt,
          a.var_capital_payment_amt,
          a.var_lab_payment_amt,
          a.var_outlier_payment_amt,
          a.var_therapy_payment_amt,
          a.var_contractual_allowance_amt,
          a.var_net_apc_service_amt,
          a.var_net_fee_schedule_amt,
          a.inpatient_outpatient_ind,
          a.reason_code_3,
          a.calc_id,
          a.account_activity_id,
          a.reason_id,
          a.account_payer_status_id,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code,
          process_date_from_etl_load.process_date,
          process_date_from_etl_load.process_timestamp
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.reimbursement_discrepancy_wrk1_rd_parallel AS a
   CROSS JOIN
     (SELECT max(etl_load.etl_load_date) AS process_date, -- Max is taken in case multiple rows where is_current = 1 are present
 CASE
     WHEN max(etl_load.etl_load_reimbursement_discrepancy_date) <> date_sub(current_date('US/Central'), interval 1 DAY) THEN CAST(trim(concat(CAST(date_add(max(etl_load.etl_load_reimbursement_discrepancy_date), interval 1 DAY) AS STRING), ' 23:59:59 PM')) AS DATETIME)
     ELSE datetime_trunc(current_datetime('US/Central'), SECOND)
 END AS process_timestamp
      FROM -- Because the PF team uses the dw_last_update_date_time column to derive the resolved date, we will update this column to match the etl_load_reimbursement_discrepancy_date if needed
  {{ params.param_parallon_ra_stage_dataset_name }}.etl_load
      WHERE etl_load.is_current = 1 ) AS process_date_from_etl_load) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
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
    discrepancy_day_cnt = date_diff(z.process_date, x.discrepancy_origination_date, DAY),
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
    dw_last_update_date_time = z.process_timestamp,
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
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimbursement_discrepancy_rd_parallel
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.cc_reimbursement_discrepancy_rd_parallel');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- Only the date from the row where is_current = 1 is needed
-- PBI 18545: Updated this so that the ETL_LOAD_DATE from the ETL_LOAD table is used to calculate the day count.  That way, if it crosses midnight, the correct starting point (not sysdate) is used for this calculation.
-- PBI 18545: This uses the logic from the cross join above since the PF team uses this date to derive the resolved date.
BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimbursement_discrepancy_rd_parallel
WHERE upper(cc_reimbursement_discrepancy_rd_parallel.coid) IN
    (SELECT -- PBI 18545: Deleting from a parallel version of CC_Reimbursement_Discrepancy
 upper(r.coid) AS coid
     FROM  {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_reimbursement_discrepancy_rd_parallel.dw_last_update_date_time <>
    (SELECT max(cc_reimbursement_discrepancy_rd_parallel_0.dw_last_update_date_time)
     FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimbursement_discrepancy_rd_parallel AS cc_reimbursement_discrepancy_rd_parallel_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

RETURN;