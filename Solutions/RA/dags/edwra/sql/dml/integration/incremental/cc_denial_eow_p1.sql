DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_denial_eow.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/CC_Denial_EOW.out;
 /***************************************************************************
 Developer: Mohammed Ashan
      Name: CC_Denial_EOW - BTEQ Script.
      Mod1: New script for weekly report. 10/10/2016.
****************************************************************************/ /*********************************************************************************
      Insert base rows
**********************************************************************************/ BEGIN
SET _ERROR_CODE = 0;

delete from {{ params.param_parallon_ra_stage_dataset_name }}.cc_denial_eow as eow
where exists (
  select 1 
  from {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1 wrk 
  where upper(rtrim(eow.unit_num)) = upper(rtrim(wrk.unit_num))
   AND eow.pat_acct_num = wrk.pat_acct_num
   AND eow.iplan_id = wrk.iplan_id
   AND eow.report_end_date = wrk.rpt_end_date
   AND eow.schema_id = 1
   AND eow.report_run_date = wrk.rpt_run_date
);

INSERT INTO  {{ params.param_parallon_ra_stage_dataset_name }}.cc_denial_eow (patient_dw_id, payor_dw_id, report_run_date, report_end_date, iplan_insurance_order_num, company_code, coid, unit_num, pat_acct_num, iplan_id, denial_status_code, patient_type_code, patient_financial_class_code, payor_financial_class_code, appeal_origination_date, appeal_level_origination_date, disposition_num, appeal_level_num, beginning_balance_amt, beginning_balance_cnt, beginning_appeal_amt, new_denial_account_amt, new_denial_account_cnt, not_true_denial_amt, write_off_denial_account_amt, overturned_account_amt, corrections_account_amt, appeal_closing_date, trans_next_party_amt, ending_balance_amt, resolved_accounts_cnt, total_charge_amt, attending_physician_name_id, account_balance_amt, discharge_date, service_code, medical_record_num, last_update_hca_3_4_id, last_update_date, appeal_deadline_date, cash_adjustment_amt, ca_adjustment_amt, root_cause, root_cause_desc, denial_code_category, disposition_code, appeal_num, sequence_number, appeal_code, appeal_code_desc, schema_id, source_system_code, vendor_cd, dw_last_update_date_time)
SELECT a.patient_dw_id,
       pyro.payor_dw_id,
       wrk.rpt_run_date,
       wrk.rpt_end_date,
       wrk.iplan_insurance_order_num,
       rcos.company_code,
       rcos.coid,
       wrk.unit_num,
       wrk.pat_acct_num,
       wrk.iplan_id,
       substr(wrk.external_code, 1, 2) AS denial_status_code,
       substr(wrk.patient_code, 1, 3) AS patient_type_code,
       CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(wrk.financial_class) AS INT64) AS patient_financial_class_code,
       CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(wrk.payer_finclass_code) AS INT64) AS payor_financial_class_code,
       wrk.min_denial_date AS appeal_origination_date,
       wrk.max_seq_create_date AS appeal_level_origination_date,
       wrk.web_denial_disp_code AS disposition_num,
       wrk.appeal_cnt AS appeal_level_num,
       0 AS beginning_balance_amt,
       wrk.existing_account AS beginning_balance_cnt,
       ROUND(wrk.min_appeal_bal_amt_beg, 3, 'ROUND_HALF_EVEN') AS beginning_appeal_amt,
       ROUND(CASE
                 WHEN wrk.new_account = 1 THEN wrk.new_amt
                 ELSE CAST(0 AS NUMERIC)
             END, 3, 'ROUND_HALF_EVEN') AS new_denial_account_amt,
       wrk.new_account AS new_denial_account_cnt,
       ROUND(wrk.not_true_denial_amt, 3, 'ROUND_HALF_EVEN') AS not_true_denial_amt,
       ROUND(wrk.writeoff_amt, 3, 'ROUND_HALF_EVEN') AS write_off_denial_account_amt,
       ROUND(wrk.overturned_amt, 3, 'ROUND_HALF_EVEN') AS overturned_account_amt,
       ROUND(wrk.corrections_amt, 3, 'ROUND_HALF_EVEN') AS corrections_account_amt,
       CASE
           WHEN coalesce(wrk.max_appeal_date_closed, DATE '1800-01-01') > wrk.rpt_end_date THEN CAST(NULL AS DATE)
           ELSE wrk.max_appeal_date_closed
       END AS appeal_closing_date,
       ROUND(wrk.xfer_next_party_amt, 3, 'ROUND_HALF_EVEN') AS trans_next_party_amt,
       ROUND(wrk.max_appeal_bal_amt, 3, 'ROUND_HALF_EVEN') AS ending_balance_amt,
       wrk.resolved_account AS resolved_accounts_cnt,
       ROUND(wrk.total_charges, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
       substr(concat(trim(wrk.attending_phys_name), ' (', trim(wrk.attending_phys_id), ')'), 1, 40) AS attending_physician_name_id,
       ROUND(wrk.account_balance, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
       wrk.discharge_date AS discharge_date,
       substr(wrk.service_code, 1, 4) AS service_code,
       substr(wrk.medical_rec_num, 1, 30) AS medical_record_num,
       substr(wrk.login_id, 1, 20) AS last_update_hca_3_4_id,
       wrk.appeal_date_modified AS last_update_date,
       wrk.deadline_date AS appeal_deadline_date,
       ROUND(wrk.cash_adj_amt, 3, 'ROUND_HALF_EVEN') AS cash_adjustment_amt,
       ROUND(wrk.contractual_allowance_amt, 3, 'ROUND_HALF_EVEN') AS ca_adjustment_amt,
       wrk.root_cause_code AS root_cause,
       substr(wrk.root_cause_description, 1, 100) AS root_cause_desc,
       substr(wrk.denial_code_category, 1, 20) AS denial_code_category,
       wrk.disposition_code AS disposition_code,
       CAST(wrk.appeal_no AS INT64) AS appeal_num,
       CAST(wrk.sequence_no AS INT64) AS sequence_number,
       wrk.appeal_code AS appeal_code,
       substr(wrk.appeal_description, 1, 100) AS appeal_code_desc,
       wrk.schema_id AS schema_id,
       'N' AS source_system_code,
       wrk.vendor_cd,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1 AS wrk
INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rcos ON upper(rtrim(rcos.unit_num)) = upper(rtrim(wrk.unit_num))
AND rcos.schema_id = wrk.schema_id
INNER JOIN  {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rcos.coid))
AND a.pat_acct_num = wrk.pat_acct_num
INNER JOIN  {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(rcos.coid))
AND upper(rtrim(pyro.company_code)) = upper(rtrim(rcos.company_code))
AND pyro.iplan_id = wrk.iplan_id
WHERE wrk.schema_id = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Account_Activity_Merge_Stage');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- --change here to EDWPF_STAGING.Payor_Organization
/*************************************************************************************
      Update Beginning Balance Amount with Ending Balance from prior month (if exists)
**************************************************************************************/ BEGIN
SET _ERROR_CODE = 0;

UPDATE  {{ params.param_parallon_ra_stage_dataset_name }}.cc_denial_eow
SET beginning_balance_amt = z.z1
FROM
  (SELECT DISTINCT eom.ending_balance_amt AS z1,
                   wrk.rpt_end_date AS z2,
                   wrk.rpt_run_date AS z3,
                   eom.unit_num AS z4,
                   eom.pat_acct_num AS z5,
                   eom.iplan_id AS z6,
                   eom.schema_id AS z7
   FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_denial_eom AS eom
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1 AS wrk ON upper(rtrim(eom.unit_num)) = upper(rtrim(wrk.unit_num))
   AND eom.pat_acct_num = wrk.pat_acct_num
   AND eom.iplan_id = wrk.iplan_id
   AND eom.schema_id = wrk.schema_id
   WHERE eom.report_end_date = date_sub(wrk.rpt_end_date, interval extract(DAY
                                                                           FROM wrk.rpt_end_date) DAY)
     AND wrk.new_account = 0 AND wrk.schema_id = 1) AS z
WHERE cc_denial_eow.report_end_date = z.z2
  AND cc_denial_eow.report_run_date = z.z3
  AND upper(rtrim(cc_denial_eow.unit_num)) = upper(rtrim(z.z4))
  AND cc_denial_eow.pat_acct_num = z.z5
  AND cc_denial_eow.iplan_id = z.z6
  AND cc_denial_eow.schema_id = z.z7;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

/**********************************************************************************
      Update Corrections Amount
**********************************************************************************/ BEGIN
SET _ERROR_CODE = 0;


UPDATE  {{ params.param_parallon_ra_stage_dataset_name }}.cc_denial_eow
SET corrections_account_amt = ROUND(z.z1 + z.z2 + z.z3 - (z.z4 + z.z5 + z.z6 + z.z7 + z.z8), 3, 'ROUND_HALF_EVEN')
FROM
  (SELECT DISTINCT coalesce(eow.corrections_account_amt, CAST(0 AS NUMERIC)) AS z1,
                   coalesce(eow.beginning_balance_amt, CAST(0 AS NUMERIC)) AS z2,
                   coalesce(eow.new_denial_account_amt, CAST(0 AS NUMERIC)) AS z3,
                   coalesce(eow.overturned_account_amt, CAST(0 AS NUMERIC)) AS z4,
                   coalesce(eow.write_off_denial_account_amt, CAST(0 AS NUMERIC)) AS z5,
                   coalesce(eow.trans_next_party_amt, CAST(0 AS NUMERIC)) AS z6,
                   coalesce(eow.not_true_denial_amt, CAST(0 AS NUMERIC)) AS z7,
                   coalesce(eow.ending_balance_amt, CAST(0 AS NUMERIC)) AS z8,
                   wrk.rpt_end_date AS z9,
                   eow.unit_num AS z10,
                   eow.pat_acct_num AS z11,
                   eow.iplan_id AS z12,
                   eow.schema_id AS z13,
                   wrk.rpt_run_date AS z14
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_denial_eow AS eow
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.denial_eow_wrk1 AS wrk ON upper(rtrim(eow.unit_num)) = upper(rtrim(wrk.unit_num))
   AND eow.pat_acct_num = wrk.pat_acct_num
   AND eow.iplan_id = wrk.iplan_id
   AND eow.report_end_date = wrk.rpt_end_date
   AND eow.schema_id = wrk.schema_id
   AND eow.report_run_date = wrk.rpt_run_date AND wrk.schema_id = 1) AS z
WHERE cc_denial_eow.report_end_date = z.z9
  AND upper(rtrim(cc_denial_eow.unit_num)) = upper(rtrim(z.z10))
  AND cc_denial_eow.pat_acct_num = z.z11
  AND cc_denial_eow.iplan_id = z.z12
  AND cc_denial_eow.schema_id = z.z13
  AND cc_denial_eow.report_run_date = z.z14;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;