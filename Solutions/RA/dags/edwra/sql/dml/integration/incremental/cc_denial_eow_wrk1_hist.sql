DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_denial_eow_wrk1_hist.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Denial_EOW_Wrk1_Hist.out;
 BEGIN
SET _ERROR_CODE = 0;


INSERT INTO  {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1_hist (schema_id, rpt_run_date, rpt_start_date, rpt_end_date, external_code, unit_num, pat_acct_num, iplan_id, iplan_insurance_order_num, deadline_date, max_seq_create_date, patient_code, financial_class, payer_finclass_code, disposition_code, web_denial_disp_code, appeal_no, total_charges, attending_phys_name, attending_phys_id, discharge_date, service_code, medical_rec_num, login_id, appeal_date_modified, root_cause_code, root_cause_description, denial_code_category, sequence_no, appeal_code, appeal_description, min_denial_date, mon_account_id, mon_payer_id, min_appeal_bal_amt_beg, max_appeal_date_closed, account_balance, max_appeal_bal_amt, new_account, new_amt, existing_account, resolved_account, not_true_denial_amt, writeoff_amt, overturned_amt, corrections_amt, xfer_next_party_amt, cash_adj_amt, contractual_allowance_amt, appeal_cnt, last_update_date_time, vendor_cd)
SELECT denial_eow_wrk1.*
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;