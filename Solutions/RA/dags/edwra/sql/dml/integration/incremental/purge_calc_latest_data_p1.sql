DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/purge_calc_latest_data.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/Mon_Account_Payer_Calc_Latest_Purge.out;
 BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_old (id, schema_id, mon_account_payer_id, calculation_no, payer_rank, is_current, mon_account_id, account_no, is_model, cers_term_id, mon_payer_id, billing_status, drg_type, drg, hipps_code, service_date_begin, service_date_end, length_of_stay, ce_patient_type_id, mon_patient_type_id, total_charges, total_payments, patient_convenience_charges, total_expected_payment, base_expected_payment, exclusion_expected_payment, cers_stoploss_id, stoploss_expected_payment, pre_cob_expected_payment, account_payer_payments, account_payer_adjustments, calculation_date, created_by, user_id_creator, noncovered_amount_pt_resp, noncovered_amount_writeoff, cers_term_id_compare, compare_expected_payment, compare_base_expected_payment, compare_exception_expected_pmt, cers_stoploss_id_compare, compare_stoploss_expected_pmt, cc_result, patient_status, admit_type, admit_source, stoploss_description, job_no, ce_patient_population_id, is_model_calculated, claim_id, late_charge_claim_id, ce_charge_model_id, mon_practitioner_id, is_deleted, drg_version, ce_state_tax_profile_id, state_tax_amt, ce_interest_profile_id, interest_amt, interest_days, original_drg, original_drg_version, original_drg_type, cers_accrual_method_id, cers_accrual_value, covered_charges, billed_charges, comp_method_choice, calc_base_choice_resolved, is_survivor, amount_due, date_amount_due_evaluated, cob_method_id, length_of_service, org_id, is_cob_capped, pre_trans_stoploss_id, pre_trans_stoploss_exp_pay, post_trans_stoploss_id, post_trans_stoploss_exp_pay, copay_amt, ce_valuation_profile_id, drg_severity_level, icd_vrsn_id, creation_date, creation_user, modification_date, modification_user, dw_last_update_date, svc_subterm_ttl_amt, hist_srvr_ind, acct_subterm_amt)
SELECT a.*
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS a
WHERE NOT EXISTS
    (SELECT 1
     FROM {{ params.param_parallon_ra_stage_dataset_name }}.mapcl_id AS b
     WHERE a.schema_id = b.schema_id
       AND a.id = b.id )
  AND a.schema_id = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS a
WHERE NOT EXISTS
    (SELECT 1
     FROM {{ params.param_parallon_ra_stage_dataset_name }}.mapcl_id AS b
     WHERE a.schema_id = b.schema_id
       AND a.id = b.id )
  AND a.schema_id = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;