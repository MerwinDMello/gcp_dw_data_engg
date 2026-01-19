DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***********************************************************************************************************************************************************************************
   Developer: Holly Ray
        Date: 9/29/2011
        Name: CC_EOR.sql
        Mod1: Based on revised EOR model.
        Mod2: Updated FAP_ID and Month13_ind. CalcFirstFive join for cases when
                      difference between calc_FAP and acct_FAP equates to more than one month
                      (only applies to calc_dates with days 1 - 5)  :  12/12/2011 hgr
        Mod3: Changed formula for EOR_Non_Covered_Charge_Amt by removing
              -1 per Fran on 6/20/2012. SW
        Mod4: Cleaned up code to make script more readable 8/23/2012 SW
        Mod5: Changed JOINS to use driving table (derived as CLMAX) on 1/14/2013 SW.
        Mod6: Enhancement for populating Outlier_Code for request id 1528 on 2/14/2013 SW.
        Mod7: Enhancement for sequestration changes and defects found for computed reimbursement
              on 5/13/2013 SW.
        Mod8: CC_DRG_CODE and EOR_DRG_CODE are not matching in some cases.  Added more condtions
              to the join for MPCL on 8/5/2013 SW.
        Mod9  : Changed the IRF column mapping for computed reimbursement
        Mod 10: Changed the logic for managed care  column mapping for computed reimbursement
        Mod 11: Changed the logic to multiply by quantity for IPF column mapping for computed reimbursement
        Mod 12: Changed the IRF column mapping for computed reimbursement  08/28/2013
        MOD 13: Remove the outlier amount from MOPS,TRICAREOP and IPBO 8/29
        Mod 14: Changed the composite flag from 1 to <>0 and is not null.8/30
        Mod 15: Changed the logic to calculate computed remb for MCARE 09/06
        Mod 16: Removed State tax amount from EOR_Gross_Reimbursement_Amt,EOR_Net_Reimbursement_Amt and added
                COALESCE(MAPYR.State_Tax_Amt,0) * -1 )as Sqstrtn_Red_Amt on 12/20/2013 MFY
        Mod 17: Removed left join to mon_account_payer_calc_summary on 2/7/2014 SW.
        Mod 18: Used Sum or Max for measures that might have multiple clac as survivors. 2/14/2014. AP
        Mod 19: Added State tax amount from EOR_Gross_Reimbursement_Amt,EOR_Net_Reimbursement_Amt and added
                COALESCE(MAPYR.State_Tax_Amt,0) * -1 )as Sqstrtn_Red_Amt on 03/01/2014 MFY
        Mod 20: Added Project Desc on 07/21/2014 MFY
        Mod 21: Fixed Computed reimbursement - missing schema id for MCARE rate schedules. 9/12/2014. FY
        Mod 22: Updated Eor_Gross_Reimbursement_Amtlogic to add the HAC amount. 10/03/2014. AP
        Mod 23: Updated the query to include with Remit Logic.(PBI781/CR112) 10/09/2014. AP
               Updated Logic for Eor_Deductible_Amt, Eor_Coinsurance_Amt, Eor_Insurance_Payment_Amt, Eor_Net_Reimbursement_Amt. 10/09/2014. AP
               Added new fields Eor_Copay_Amt, Pat_Resp_Non_Covered_Chrg_Amt, Total_Actual_Pat_Resp_Amt, Expected_Insurance_Payment_Amt.10/09/2014. AP
        Mod 24: Modified Eor_Non_Bill_Charge_Amt to use Mon_Account Table rather than mapcl.(CR136) 10/10/2014. AP.
                Modified Eor_Cmptd_Reimbursement_Amt for medicaid and managed care to sum up the amounts from multiple calcs. (CR136) 10/13/2014. AP.
        Mod 25: Updated the query to be in sync with prod version. Project Desc is added, was missed out during the migration of previous change (Mod 24).10/21/2014. AP.
	    Mod 26: Removed CAST on Patient Account number on 1/14/2015 AS.
        Mod 27: Added back the diagnostic sessions commands on 3/9/2015 FY..
		Mod 28: Modified Ma.Drg Eor_Drg_Code to Mpcl.Drg Eor_Drg_Code per Fran's request in Incident 4518:DRG Codes EDW (ICD-10) - JC  12/8/2015
		Mod 29: Changed delete to only consider active coids on 1/30/2018 SW.
		Mod 30: PBI16380 Logic for calculating Eor_Non_Covered_Charge_Amt was incorrect. Should be Ma.Billed_Charges - Mapyr.Covered_Charges_Inst. SW 05/15/2018
	Mod31:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod32: commented no hash join DIagnostics statements and added one more diagnostic statement as recommended by teradata DBA's PT 12/19/2018
	Mod33: Logic has been added for TRICARE IRF as that of MIRF.Modified to use name column from cers_profile instead of rate_schedule_name from Mon_Account_Payer .
				Modified logic to calculate Wage_Adj_Rate column.Added condition for tricare_irf while calculating outlier_code column
		Added trim function to crpf.name column to have all valid records refer PBI 18374 and PBI 20158 PT 1/4/2019
		Mod34: modified logic for populating Eor_reimbursement_computed_amt for MCARE added hac_adj_amt (-1) PBI19954 AM 1/24/2019
		Mod35: Added more election strings for Ce_service and CE_Exclusion tables like any AM 2/8/2019
	Mod36: Added collect stats for CC_EOR table AM 3/6/2019
	Mod37:Logic for Eor_reimbursement_computed_amt for MIPS has been changed  PBI19556 AM 4/5/2019
	Mod38:Logic for Eor_reimbursement_computed_amt for MOPS  and IPBO has been changed PBI19957 AM 5/18/2019
	mod39:Logic Eor_reimbursement_computed_amt for MIPF  and MSNF has been changed PBI19957 AM 6/20/2019
	Mod40:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
	Mod41: Added Audit Merge 10292020
***********************************************************************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA140;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- --DIAGNOSTIC nohashjoin ON FOR SESSION;
 -- DIAGNOSTIC "prpdcostoption=1" ON FOR SESSION;
 -- DIAGNOSTIC noprodjoin ON FOR SESSION;
 -- DIAGNOSTIC noviewfold ON FOR SESSION;
 -- DIAGNOSTIC noparallel ON FOR SESSION;
 -- Diagnostic "prpdmode=1" on for session;
 -- Locking edwpf_staging.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;


--CREATE
--TEMPORARY TABLE clmax CLUSTER BY mon_account_payer_id,
--                                 schema_id AS
--SELECT mapcl.mon_account_payer_id,
--       mapcl.schema_id,
--       mapcl.payer_rank,
--       mapcl.account_no AS account_no,
--       max(mapcl.id) AS calc_id,
--       DATE(max(mapcl.calculation_date)) AS calc_date,
--       date_add(DATE(max(mapcl.calculation_date)), interval -1 MONTH) AS calc_date_1,
--       sum(coalesce(mapcl.total_charges, CAST(0 AS NUMERIC))) AS total_charges,
--       sum(coalesce(mapcl.billed_charges, CAST(0 AS NUMERIC))) AS billed_charges,
--       sum(coalesce(mapcl.base_expected_payment, CAST(0 AS NUMERIC))) AS base_expected_payment,
--       sum(coalesce(mapcl.exclusion_expected_payment, CAST(0 AS NUMERIC))) AS exclusion_expected_payment,
--       sum(coalesce(mapcl.length_of_service, CAST(0 AS NUMERIC))) AS length_of_service,
--       sum(coalesce(mapcl.total_expected_payment, CAST(0 AS NUMERIC))) AS total_expected_payment
--FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
--WHERE mapcl.is_survivor = 1
--GROUP BY 1,
--         2,
--         3,
--         4;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


--CREATE
--TEMPORARY TABLE ma CLUSTER BY id,
--                              schema_id AS
--SELECT ma.*,
--       macp.cost_report_org_id,
--       macp.cost_report_year,
--       mapd.start_date,
--       mapd.end_date,
--       mapd.id AS mapd_id
--FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
--LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS macp ON coalesce(ma.mon_accounting_period_id, CAST(0 AS NUMERIC)) = macp.mon_accounting_period_id
--AND ma.org_id_provider = macp.cost_report_org_id
--AND ma.schema_id = macp.schema_id
--LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS mapd ON ma.mon_accounting_period_id = mapd.id
--AND ma.schema_id = mapd.schema_id;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


--CREATE
--TEMPORARY TABLE mpcl CLUSTER BY id,
--                                schema_id AS
--SELECT mpcl.*,
--       clm.date_billed,
--       clm.statement_to,
--       cterm.cers_profile_id,
--       cterm.effective_date_begin
--FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl
--LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.claim AS clm ON mpcl.claim_id = clm.id
--AND mpcl.schema_id = clm.schema_id
--LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS cterm ON mpcl.cers_term_id = cterm.id
--AND mpcl.schema_id = cterm.schema_id;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

--CREATE
--TEMPORARY TABLE calc_fap AS SELECT * FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- --------------------------------------
-- DIAGNOSTIC "prpdcostoption=1" ON FOR SESSION;
 -- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- COLLECT STATISTICS is not supported in this dialect.
-- --------------------------------------
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_stg AS mt USING
  (SELECT DISTINCT company_cd AS company_cd,
                   coido AS coido,
                   a.patient_dw_id AS patient_dw_id,
                   pyro.payor_dw_id AS payor_dw_id,
                   iplan_insurance_order_num AS iplan_insurance_order_num,
                   clmax.calc_date AS eor_log_date,
                   substr(concat('INS', trim(format('%11d', CAST(clmax.payer_rank AS INT64)))), 1, 4) AS log_id,
                   row_number() OVER (PARTITION BY upper(company_cd),
                                                   upper(coido),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id,
                                                   iplan_insurance_order_num,
                                                   clmax.calc_date,
                                                   upper(pyro.log_id),
                                                   pyro.eff_from_date
                                      ORDER BY clmax.calc_id) AS log_sequence_num,
                                     clmax.calc_date AS eff_from_date,
                                     reforg.unit_num AS unit_num,
                                     CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) AS NUMERIC) AS eor_pat_acct_num,
                                     CASE
                                         WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                                     END AS eor_iplan_id,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapyr.misc_char01) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS financial_class_code,
                                     ma.misc_date01 AS final_bill_date,
                                     CASE
                                         WHEN upper(trim(ma.misc_char05)) IN('Y',
                                                                              'C') THEN mpcl.statement_to
                                         ELSE ma.misc_date02
                                     END AS ar_bill_thru_date,
                                     CASE
                                         WHEN upper(trim(ma.misc_char05)) IN('Y',
                                                                              'C') THEN mpcl.date_billed
                                         ELSE mapyr.billed_date
                                     END AS ar_bill_drop_date,
                                     mpcl.effective_date_begin AS contract_begin_date,
                                     substr(mapyr.misc_char04, 1, 2) AS bill_reason_code,
                                     clmax.calc_date AS eor_transaction_date,
                                     substr(mpcl.drg, 1, 5) AS eor_drg_code,
                                     substr(mpcl.drg, 1, 5) AS cc_drg_code,
                                     CAST(ROUND(max_svc_gvt.cost_wt, 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS cc_drg_weight,
                                     substr(mapcirf.hipps_code, 1, 5) AS eor_hipps_code,
                                     substr(mapcirf.hipps_code, 1, 2) AS cc_cmg_code,
                                     ROUND(mapcirf.irf_comorbid_tier_multiplier, 6, 'ROUND_HALF_EVEN') AS cc_cmg_weight,
                                     CAST(clmax.length_of_service AS INT64) AS eor_covered_days_num,
                                     ROUND(ma.total_charges, 3, 'ROUND_HALF_EVEN') AS eor_total_charge_amt,
                                     ROUND(coalesce(ma.billed_charges, CAST(0 AS NUMERIC)) - coalesce(mapyr.covered_charges_inst, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS eor_non_covered_charge_amt,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(CASE
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MIPS IP%' THEN regexp_replace(format('%#40.6f', CASE
                                                                                                                                                                                                           WHEN CASE
                                                                                                                                                                                                                    WHEN mapyr.payer_rank = 2
                                                                                                                                                                                                                         AND upper(trim(mpcl.cc_result)) = 'SUCCESS'
                                                                                                                                                                                                                         AND mpcl.cob_method_id IS NOT NULL
                                                                                                                                                                                                                         AND mpcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                                                                                                                                                                                                    ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                END = 0 THEN coalesce(svc_gvt.oper_fed_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.val_based_adjmt_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.readm_adjmt_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.gvt_scp_payment, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.mdh_add_on_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.low_vol_add_on_amt, CAST(0 AS BIGNUMERIC)) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - (coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                           ELSE coalesce(svc_gvt.oper_fed_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.val_based_adjmt_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.readm_adjmt_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.gvt_scp_payment, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.mdh_add_on_amt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.low_vol_add_on_amt, CAST(0 AS BIGNUMERIC)) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + (coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                       END), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MIRF%'
                                                                                                                      OR upper(trim(crpf.name)) LIKE 'GOV TRICARE IRF%' THEN CAST(calc_irf.wage_adj_rate AS STRING)
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MIPF%' THEN regexp_replace(format('%#40.2f', CASE
                                                                                                                                                                                                        WHEN CASE
                                                                                                                                                                                                                 WHEN mapyr.payer_rank = 2
                                                                                                                                                                                                                      AND upper(trim(mpcl.cc_result)) = 'SUCCESS'
                                                                                                                                                                                                                      AND mpcl.cob_method_id IS NOT NULL
                                                                                                                                                                                                                      AND mpcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                                                                                                                                                                                                 ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                             END = 0 THEN svc_gvt.ipf_pmt - coalesce(ect.not_ect_amount, CAST(0 AS BIGNUMERIC)) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - (coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                        ELSE svc_gvt.ipf_pmt - coalesce(ect.not_ect_amount, CAST(0 AS BIGNUMERIC)) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + (coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                    END), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MSNF%'
                                                                                                                      OR upper(trim(crpf.name)) LIKE 'GOV MSWB%' THEN regexp_replace(format('%#40.2f', CASE
                                                                                                                                                                                                           WHEN CASE
                                                                                                                                                                                                                    WHEN mapyr.payer_rank = 2
                                                                                                                                                                                                                         AND upper(trim(mpcl.cc_result)) = 'SUCCESS'
                                                                                                                                                                                                                         AND mpcl.cob_method_id IS NOT NULL
                                                                                                                                                                                                                         AND mpcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                                                                                                                                                                                                    ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                END = 0 THEN svc_gvt.exp_pmt + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - (coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                           ELSE svc_gvt.exp_pmt + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + (coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                       END), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV IPBO%' THEN regexp_replace(format('%#40.2f', CASE
                                                                                                                                                                                                        WHEN CASE
                                                                                                                                                                                                                 WHEN mapyr.payer_rank = 2
                                                                                                                                                                                                                      AND upper(trim(mpcl.cc_result)) = 'SUCCESS'
                                                                                                                                                                                                                      AND mpcl.cob_method_id IS NOT NULL
                                                                                                                                                                                                                      AND mpcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                                                                                                                                                                                                 ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                             END = 0 THEN coalesce(calc_apc.expected_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.exp_pmt, CAST(0 AS BIGNUMERIC)) - (coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.apcoutlieramt, CAST(0 AS BIGNUMERIC))) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - (coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                        ELSE coalesce(calc_apc.expected_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.exp_pmt, CAST(0 AS BIGNUMERIC)) - (coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.apcoutlieramt, CAST(0 AS BIGNUMERIC))) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + (coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                    END), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MOPS OP%'
                                                                                                                      OR upper(trim(crpf.name)) LIKE 'GOV MCRO%' THEN regexp_replace(format('%#40.2f', CASE
                                                                                                                                                                                                           WHEN CASE
                                                                                                                                                                                                                    WHEN mapyr.payer_rank = 2
                                                                                                                                                                                                                         AND upper(trim(mpcl.cc_result)) = 'SUCCESS'
                                                                                                                                                                                                                         AND mpcl.cob_method_id IS NOT NULL
                                                                                                                                                                                                                         AND mpcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                                                                                                                                                                                                    ELSE CAST(0 AS NUMERIC)
                                                                                                                                                                                                                END = 0 THEN coalesce(calc_apc.expected_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.exp_pmt, CAST(0 AS BIGNUMERIC)) - (coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.apcoutlieramt, CAST(0 AS BIGNUMERIC))) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - (coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                           ELSE coalesce(calc_apc.expected_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.exp_pmt, CAST(0 AS BIGNUMERIC)) - (coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.apcoutlieramt, CAST(0 AS BIGNUMERIC))) + coalesce(pass.passthrough_amount, CAST(0 AS BIGNUMERIC)) + coalesce(mpcl.acct_subterm_amt, CAST(0 AS NUMERIC)) + (coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC)))
                                                                                                                                                                                                       END), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE OP%' THEN regexp_replace(format('%#40.2f', coalesce(calc_triapc.expected_pmt, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt2.exp_pmt, CAST(0 AS BIGNUMERIC))), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV MEDICAID IP%'
                                                                                                                      OR upper(trim(crpf.name)) LIKE 'GOV MEDICAID OP%' THEN regexp_replace(format('%#40.2f', clmax.total_expected_payment), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE IP%' THEN regexp_replace(format('%#40.6f', calc_tridrg.drg_pmt), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 WHEN upper(trim(crpf.name)) NOT LIKE 'GOV%'
                                                                                                                      OR trim(crpf.name) IS NULL THEN regexp_replace(format('%#40.2f', coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)) + coalesce(svc_gvt.hac_adjmt_amt * -1, CAST(0 AS BIGNUMERIC)) + coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)) - coalesce(cename.expected_pmt, CAST(0 AS BIGNUMERIC)) - coalesce(mri_ct_amb_exp.srv_mri_ct_amb_exp_payment, CAST(0 AS BIGNUMERIC)) - coalesce(hcd_exp.srv_hcd_exp_payment, CAST(0 AS BIGNUMERIC)) - coalesce(imp_exp.srv_imp_exp_payment, CAST(0 AS BIGNUMERIC))), r'^(.*?)(-)?(\..*)', r'\1\2\3')
                                                                                                                 ELSE ' '
                                                                                                             END) AS NUMERIC), 3, 'ROUND_HALF_EVEN') AS eor_cmptd_reimbursement_amt,
                                     CAST(ROUND(CASE
                                                    WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_deductible, CAST(0 AS BIGNUMERIC))
                                                    ELSE coalesce(mapyr.deductible, CAST(0 AS NUMERIC))
                                                END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_deductible_amt,
                                     CAST(ROUND(CASE
                                                    WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_coinsurance, CAST(0 AS BIGNUMERIC))
                                                    ELSE coalesce(mapyr.coinsurance, CAST(0 AS NUMERIC))
                                                END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_coinsurance_amt,
                                     CAST(ROUND(CASE
                                                    WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_copay, CAST(0 AS BIGNUMERIC))
                                                    ELSE coalesce(mapyr.copay, CAST(0 AS NUMERIC))
                                                END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_copay_amt,
                                     CAST(ROUND(CASE
                                                    WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_pt_resp_non_covered_amt, CAST(0 AS BIGNUMERIC))
                                                    ELSE CAST(0 AS BIGNUMERIC)
                                                END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS pat_resp_non_covered_chg_amt,
                                     ROUND(coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.total_pt_responsibility, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS expected_insurance_payment_amt,
                                     ROUND(coalesce(mapyr.total_pt_responsibility, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS total_actual_pat_resp_amt,
                                     ROUND(CASE
                                               WHEN rmt.remit_received_flag = 1 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.total_pt_responsibility, CAST(0 AS NUMERIC))
                                               ELSE coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.estimated_pt_responsibility, CAST(0 AS NUMERIC))
                                           END, 3, 'ROUND_HALF_EVEN') AS eor_insurance_payment_amt,
                                     ROUND(CASE
                                               WHEN clmax.payer_rank <> 1 THEN CAST(0 AS NUMERIC)
                                               ELSE mapyr.expected_contractual
                                           END, 3, 'ROUND_HALF_EVEN') AS eor_contractual_allowance_amt,
                                     CAST(ROUND(CASE
                                                    WHEN mapyr.payer_rank = 1
                                                         AND upper(trim(crpf.name)) LIKE 'GOV MIPS%'
                                                         AND (upper(trim(crpf.name)) NOT LIKE 'GOV MIRF%'
                                                              OR upper(trim(crpf.name)) NOT LIKE 'GOV TRICARE IRF%') THEN svc_gvt.gvt_operating_cost
                                                    WHEN upper(trim(crpf.name)) LIKE 'GOV MIRF%'
                                                         OR upper(trim(crpf.name)) LIKE 'GOV TRICARE IRF%' THEN mapyr.covered_charges_inst
                                                    ELSE CAST(0 AS BIGNUMERIC)
                                                END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_cost_of_service_amt,
                                     substr(CASE
                                                WHEN trim(coalesce(ma.cost_report_year, format('%4d', 0))) = trim(coalesce(ma_13.cost_report_year, format('%4d', 0))) THEN 'N'
                                                ELSE 'Y'
                                            END, 1, 1) AS month_13_ind,
                                     substr(CASE
                                                WHEN svc_gvt.gvt_out_pmt <> 0 THEN 'C'
                                                WHEN mapyr.payer_rank = 2
                                                     AND upper(trim(ma.billing_status)) = 'B' THEN 'C'
                                                WHEN mapyr.payer_rank = 3
                                                     AND upper(trim(ma.billing_status)) = 'B' THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIPS IP%'
                                                     AND upper(trim(ma.financial_class)) = '1'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND upper(trim(mpt.code)) <> 'IMB'
                                                     AND coalesce(svc_gvt.gvt_out_pmt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIPF%'
                                                     AND upper(trim(ma.financial_class)) = '1'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND upper(trim(mpt.code)) <> 'IMB'
                                                     AND coalesce(svc_gvt.gvt_outperdiemaddon, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIRF%'
                                                     AND upper(trim(ma.financial_class)) = '2'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND upper(trim(mpt.code)) <> 'IMB'
                                                     AND coalesce(calc_irf.irf_outlierpmt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE IP STANDARD%'
                                                     AND upper(trim(ma.financial_class)) = '6'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND upper(trim(mpt.code)) <> 'IMB'
                                                     AND coalesce(calc_tridrg.standard_cst_outlier_pay, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV IPBO%'
                                                     AND upper(trim(ma.financial_class)) = '1'
                                                     AND upper(trim(mpt.code)) = 'IMB'
                                                     AND coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0
                                                     OR coalesce(calc_apcdtl.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MOPS OP%'
                                                     AND upper(trim(ma.financial_class)) = '1'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND coalesce(calc_apc.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0
                                                     OR coalesce(calc_apcdtl.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE OP STANDARD%'
                                                     AND upper(trim(ma.financial_class)) = '6'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'O'
                                                     AND coalesce(calc_triapc.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0
                                                     OR coalesce(calc_tricomp.outlier_pmt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE IRF%'
                                                     AND upper(trim(ma.financial_class)) = '6'
                                                     AND upper(trim(mpt.ip_op_ind)) = 'I'
                                                     AND upper(trim(mpt.code)) <> 'IMB'
                                                     AND coalesce(svc_gvt.gvt_outperdiemaddon, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                ELSE ' '
                                            END, 1, 10) AS outlier_code,
                                     ROUND(mapyr.misc_amt03, 3, 'ROUND_HALF_EVEN') AS auto_post_amt,
                                     substr(mapyr.misc_char03, 1, 1) AS auto_post_ind,
                                     substr(CASE
                                                WHEN mpcl.drg_version <> 0
                                                     AND upper(trim(cdv.code_type)) = 'APDRG'
                                                     AND upper(trim(substr(cdv.version_name, 1, 14))) = 'APDRG VERSION' THEN concat('APDRGV', substr(trim(cdv.version_name), 15, 2))
                                                WHEN mpcl.drg_version <> 0
                                                     AND upper(trim(cdv.code_type)) = 'DRG'
                                                     AND upper(trim(substr(cdv.version_name, 1, 6))) = 'CMS FY' THEN concat('CMS', substr(trim(cdv.version_name), 7, 2), 'V', substr(trim(cdv.version_name), length(trim(cdv.version_name)) - 1, 2))
                                                WHEN mpcl.drg_version <> 0
                                                     AND upper(trim(cdv.code_type)) = 'CHAMPUSDRG'
                                                     AND upper(trim(substr(cdv.version_name, 1, 3))) = 'TRI' THEN concat('TRI', substr(trim(cdv.version_name), length(trim(cdv.version_name)) - 3, 4))
                                                ELSE CAST(NULL AS STRING)
                                            END, 1, 40) AS drg_table_id,
                                     substr(mapyr.misc_yn01, 1, 1) AS opps_ind,
                                     substr(mpt.ip_op_ind, 1, 1) AS inpatient_outpatient_ind,
                                     CAST(ma.misc_amt03 AS INT64) AS visit_count,
                                     CAST(ROUND(coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) + coalesce(mapyr.state_tax_amt * -1, CAST(0 AS NUMERIC)) + coalesce(svc_gvt.hac_adjmt_amt * -1, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_gross_reimbursement_amt,
                                     CAST(ROUND(coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)) - CASE
                                                                                                                                                                     WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_deductible, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                     ELSE coalesce(mapyr.deductible, CAST(0 AS NUMERIC))
                                                                                                                                                                 END - CASE
                                                                                                                                                                           WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_coinsurance, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                           ELSE coalesce(mapyr.coinsurance, CAST(0 AS NUMERIC))
                                                                                                                                                                       END - CASE
                                                                                                                                                                                 WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_copay, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                                 ELSE coalesce(mapyr.copay, CAST(0 AS NUMERIC))
                                                                                                                                                                             END - CASE
                                                                                                                                                                                       WHEN rmt.remit_received_flag = 1 THEN coalesce(rmt.ra_pt_resp_non_covered_amt, CAST(0 AS BIGNUMERIC))
                                                                                                                                                                                       ELSE CAST(0 AS BIGNUMERIC)
                                                                                                                                                                                   END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS eor_net_reimbursement_amt,
                                     ROUND(coalesce(ma.total_charges, CAST(0 AS NUMERIC)) - coalesce(ma.billed_charges, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS eor_non_bill_charge_amt,
                                     ROUND(coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)) * -1, 3, 'ROUND_HALF_EVEN') AS sqstrtn_red_amt,
                                     substr(trim(crpf.name), 1, 100) AS model_name,
                                     substr(mpj.description, 1, 100) AS project_desc,
                                     CAST(ma.ipf_per_diem_begin_day AS NUMERIC) AS eor_ipf_interrupted_day_stay,
                                     CASE
                                        WHEN mapyr.calc_result = 1 THEN 'Y'
                                        ELSE 'N'
                                     END AS calc_success_ind,
                                     CASE
                                        WHEN mpcl.is_current = 1 THEN 'Y'
                                        ELSE 'N'
                                      END AS latest_calc_ind,
                                     substr(ma.misc_char03, 1, 1) AS interim_bill_ind,
                                     CASE
                                         WHEN upper(trim(ma.billing_status)) = 'B' THEN CASE
                                                                                             WHEN extract(DAY
                                                                                                          FROM clmax.calc_date) BETWEEN 1 AND 5 THEN CASE
                                                                                                                                                         WHEN ma.mapd_id < calc_fap.id THEN calcfirstfive_fap.id
                                                                                                                                                         WHEN ma.mapd_id >= calc_fap.id THEN ma.mapd_id
                                                                                                                                                     END
                                                                                             ELSE CASE
                                                                                                      WHEN clmax.calc_date BETWEEN ma.start_date AND ma.end_date THEN ma.mapd_id
                                                                                                      WHEN CAST(clmax.calc_date AS DATETIME) BETWEEN CAST(date_add(ma.end_date, interval 1 DAY) AS DATETIME) AND CAST(calc_fap.close_date AS DATETIME) + INTERVAL 12 HOUR THEN CASE
                                                                                                                                                                                                                                                                                   WHEN ma.mapd_id <= calc_fap.id THEN calc_fap.id
                                                                                                                                                                                                                                                                                   WHEN ma.mapd_id > calc_fap.id THEN ma.mapd_id
                                                                                                                                                                                                                                                                               END
                                                                                                  END
                                                                                         END
                                     END AS financial_period_id,
                                     mapyr.mon_reason_id AS reason_id,
                                     clmax.calc_id AS calc_id,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                     'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_project AS mpj ON mapyr.mon_project_id = mpj.id
   AND mapyr.schema_id = mpj.schema_id
   INNER JOIN (SELECT mapcl.mon_account_payer_id,
       mapcl.schema_id,
       mapcl.payer_rank,
       mapcl.account_no AS account_no,
       max(mapcl.id) AS calc_id,
       DATE(max(mapcl.calculation_date)) AS calc_date,
       date_add(DATE(max(mapcl.calculation_date)), interval -1 MONTH) AS calc_date_1,
       sum(coalesce(mapcl.total_charges, CAST(0 AS NUMERIC))) AS total_charges,
       sum(coalesce(mapcl.billed_charges, CAST(0 AS NUMERIC))) AS billed_charges,
       sum(coalesce(mapcl.base_expected_payment, CAST(0 AS NUMERIC))) AS base_expected_payment,
       sum(coalesce(mapcl.exclusion_expected_payment, CAST(0 AS NUMERIC))) AS exclusion_expected_payment,
       sum(coalesce(mapcl.length_of_service, CAST(0 AS NUMERIC))) AS length_of_service,
       sum(coalesce(mapcl.total_expected_payment, CAST(0 AS NUMERIC))) AS total_expected_payment
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
WHERE mapcl.is_survivor = 1
GROUP BY 1,
         2,
         3,
         4) as clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   INNER JOIN (SELECT mpcl.*,
       clm.date_billed,
       clm.statement_to,
       cterm.cers_profile_id,
       cterm.effective_date_begin
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.claim AS clm ON mpcl.claim_id = clm.id
AND mpcl.schema_id = clm.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS cterm ON mpcl.cers_term_id = cterm.id
AND mpcl.schema_id = cterm.schema_id) as mpcl ON clmax.calc_id = mpcl.id
   AND clmax.schema_id = mpcl.schema_id
   AND mpcl.service_date_begin < current_date('US/Central')
   INNER JOIN (SELECT ma.*,
       macp.cost_report_org_id,
       macp.cost_report_year,
       mapd.start_date,
       mapd.end_date,
       mapd.id AS mapd_id
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS macp ON coalesce(ma.mon_accounting_period_id, CAST(0 AS NUMERIC)) = macp.mon_accounting_period_id
AND ma.org_id_provider = macp.cost_report_org_id
AND ma.schema_id = macp.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS mapd ON ma.mon_accounting_period_id = mapd.id
AND ma.schema_id = mapd.schema_id) as ma ON mapyr.mon_account_id = ma.id
   AND mapyr.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON reforg.org_id = ma.org_id_provider
   AND reforg.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
   AND mapyr.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON ma.mon_patient_type_id = mpt.id
   AND ma.schema_id = mpt.schema_id
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
 sum(coalesce(mapcsvc.val_based_adjmt_amt, CAST(0 AS NUMERIC))) AS val_based_adjmt_amt,
 sum(coalesce(mapcsvc.readm_adjmt_amt, CAST(0 AS NUMERIC))) AS readm_adjmt_amt,
 sum(coalesce(mapcsvc.gvt_scp_payment, CAST(0 AS NUMERIC))) AS gvt_scp_payment,
 sum(coalesce(mapcsvc.mdh_add_on_amt, CAST(0 AS NUMERIC))) AS mdh_add_on_amt,
 sum(coalesce(mapcsvc.low_vol_add_on_amt, CAST(0 AS NUMERIC))) AS low_vol_add_on_amt,
 mapcl.mon_account_payer_id,
 mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 14,
               15) AS svc_gvt ON svc_gvt.mon_account_payer_id = clmax.mon_account_payer_id
   AND svc_gvt.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc2.apc_outlier_amount, CAST(0 AS NUMERIC))) AS apcoutlieramt,
             sum(coalesce(mapcsvc2.expected_payment, CAST(0 AS NUMERIC))) AS exp_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc2
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc2.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc2.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      WHERE mapcsvc2.apc_composite_flag <> 0
        AND mapcsvc2.apc_composite_flag IS NOT NULL
      GROUP BY 3,
               4) AS svc_gvt2 ON svc_gvt2.mon_account_payer_id = clmax.mon_account_payer_id
   AND svc_gvt2.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT max(coalesce(mapcsvc.cost_weight, CAST(0 AS NUMERIC))) AS cost_wt,
             mapcsvc.mon_acct_payer_calc_summary_id,
             mapcsvc.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 2,
               3) AS max_svc_gvt ON max_svc_gvt.mon_acct_payer_calc_summary_id = clmax.calc_id
   AND max_svc_gvt.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcfs.expected_payment, CAST(0 AS NUMERIC))) AS exp_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_fs AS mapcfs
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcfs.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcfs.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 2,
               3) AS fs ON fs.mon_account_payer_id = clmax.mon_account_payer_id
   AND fs.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(apc.expected_payment, CAST(0 AS NUMERIC))) AS expected_pmt,
             sum(coalesce(apc.apc_outlier_payment, CAST(0 AS NUMERIC))) AS outlier_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc AS apc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON apc.mon_acct_payer_calc_summary_id = mapcl.id
      AND apc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 3,
               4) AS calc_apc ON calc_apc.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_apc.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(apcdtl.expected_payment, CAST(0 AS NUMERIC))) AS expected_pmt,
             sum(coalesce(apcdtl.apc_outlier_payment, CAST(0 AS NUMERIC))) AS outlier_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_apc_comp_dtl AS apcdtl
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON apcdtl.mon_acct_payer_calc_summary_id = mapcl.id
      AND apcdtl.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 3,
               4) AS calc_apcdtl ON calc_apcdtl.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_apcdtl.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(triapc.expected_payment, CAST(0 AS NUMERIC))) AS expected_pmt,
             sum(coalesce(triapc.tricare_apc_outlier_payment, CAST(0 AS NUMERIC))) AS outlier_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_apc AS triapc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON triapc.mon_acct_payer_calc_summary_id = mapcl.id
      AND triapc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 3,
               4) AS calc_triapc ON calc_triapc.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_triapc.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(tricomp.expected_payment, CAST(0 AS NUMERIC))) AS expected_pmt,
             sum(coalesce(tricomp.tricare_apc_outlier_payment, CAST(0 AS NUMERIC))) AS outlier_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tricomp AS tricomp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON tricomp.mon_acct_payer_calc_summary_id = mapcl.id
      AND tricomp.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 3,
               4) AS calc_tricomp ON calc_tricomp.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_tricomp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(tridrg.drg_payment, CAST(0 AS NUMERIC))) AS drg_pmt,
             sum(coalesce(tridrg.standard_cost_outlier_pay, CAST(0 AS NUMERIC))) AS standard_cst_outlier_pay,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_tri_drg AS tridrg
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON tridrg.mon_acct_payer_summary_id = mapcl.id
      AND tridrg.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 3,
               4) AS calc_tridrg ON calc_tridrg.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_tridrg.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(irf.outlier_payment, CAST(0 AS NUMERIC))) AS irf_outlierpmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id,
             CASE
                 WHEN sum(coalesce(irf.is_irf_transfer_calc, NUMERIC '0')) <> 0 THEN sum(coalesce(irf.irf_transfer_shortstay_amount, NUMERIC '0')) - sum(coalesce(irf.irf_xfer_teaching_status_adj, NUMERIC '0')) - sum(coalesce(irf.wage_xfer_rural_lip_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                 ELSE CASE
                          WHEN sum(coalesce(irf.wage_rural_adj_rate, NUMERIC '0')) <> 0 THEN sum(coalesce(irf.wage_rural_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                          ELSE sum(coalesce(irf.wage_adj_rate, NUMERIC '0')) + sum(coalesce(mapcl.acct_subterm_amt, NUMERIC '0'))
                      END
             END AS wage_adj_rate
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf AS irf
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON irf.mon_acct_payer_calc_summary_id = mapcl.id
      AND irf.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 2,
               3) AS calc_irf ON calc_irf.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_irf.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS expected_pmt,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ce ON mapcsvc.ce_service_id = ce.id
      AND mapcsvc.schema_id = ce.schema_id
      WHERE upper(ce.name) LIKE 'MRI%'
        OR upper(ce.name) LIKE 'CT SCAN%'
        OR upper(ce.name) LIKE '%CAT %'
        OR upper(ce.name) LIKE 'CT %'
        OR upper(ce.name) LIKE 'CT/%'
        OR upper(ce.name) LIKE ' CT %'
        OR upper(ce.name) LIKE '%/CT %'
        OR upper(ce.name) LIKE '% CT'
        OR upper(ce.name) LIKE '% CT %'
        OR upper(ce.name) LIKE 'AMB%'
        OR upper(ce.name) LIKE 'HCD%'
        OR upper(ce.name) LIKE 'HCD%'
        OR upper(ce.name) LIKE '%HCD'
        OR upper(ce.name) LIKE '%HCD %'
        OR upper(ce.name) LIKE 'HIGH COST DRUGS%'
        OR upper(ce.name) LIKE '%HIGH COST DRUGS%'
        OR upper(ce.name) LIKE 'HIGH COST DRUGS'
        OR upper(ce.name) LIKE 'IMPLANTS%'
        OR upper(ce.name) LIKE 'IMPLANT%'
        OR upper(ce.name) LIKE '%IMPLANT'
        OR upper(ce.name) LIKE '%IMPLANT%'
      GROUP BY 2,
               3) AS cename ON cename.mon_account_payer_id = clmax.mon_account_payer_id
   AND cename.schema_id = clmax.schema_id
   LEFT OUTER JOIN /*  Any (
                                 'MRI%'
                                ,'CT Scan%'
                                ,'AMB%'
                                ,'HCD%'
                                ,'Implants%'
                               )*/
     (SELECT sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS amount,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'MRI%'
        OR upper(cexl.description) LIKE 'CT SCAN%'
        OR upper(cexl.description) LIKE '%CAT %'
        OR upper(cexl.description) LIKE 'CT %'
        OR upper(cexl.description) LIKE 'CT/%'
        OR upper(cexl.description) LIKE ' CT %'
        OR upper(cexl.description) LIKE '%/CT %'
        OR upper(cexl.description) LIKE '% CT'
        OR upper(cexl.description) LIKE '% CT %'
        OR upper(cexl.description) LIKE 'AMB%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE '%HCD'
        OR upper(cexl.description) LIKE '%HCD %'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE '%HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS'
        OR upper(cexl.description) LIKE 'IMPLANTS%'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE '%IMPLANT'
        OR upper(cexl.description) LIKE '%IMPLANT%'
      GROUP BY 2,
               3) AS cexcl ON cexcl.mon_account_payer_id = clmax.mon_account_payer_id
   AND cexcl.schema_id = clmax.schema_id
   LEFT OUTER JOIN /*Any (
                                 'MRI%'
                                ,'CT Scan%'
                                ,'AMB%'
                                ,'HCD%'
                                ,'Implants%'
                               )*/
     (SELECT sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_mri_ct_amb_exp_payment,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'AMB%'
        OR upper(cexl.description) LIKE 'MRI%'
        OR upper(cexl.description) LIKE 'CAT%'
        OR upper(cexl.description) LIKE 'CT%'
        OR upper(cexl.description) LIKE 'CT SCAN%'
        OR upper(cexl.description) LIKE '%CAT %'
        OR upper(cexl.description) LIKE 'CT %'
        OR upper(cexl.description) LIKE 'CT/%'
        OR upper(cexl.description) LIKE ' CT %'
        OR upper(cexl.description) LIKE '%/CT %'
        OR upper(cexl.description) LIKE '% CT'
        OR upper(cexl.description) LIKE '% CT %'
      GROUP BY 2,
               3) AS mri_ct_amb_exp ON mri_ct_amb_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND mri_ct_amb_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_hcd_exp_payment,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST PHARMACEUTICALS%'
        OR upper(cexl.description) LIKE 'HCD%'
        OR upper(cexl.description) LIKE '%HCD'
        OR upper(cexl.description) LIKE '%HCD %'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE '%HIGH COST DRUGS%'
        OR upper(cexl.description) LIKE 'HIGH COST DRUGS'
      GROUP BY 2,
               3) AS hcd_exp ON hcd_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND hcd_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcecl.amount, CAST(0 AS NUMERIC))) AS srv_imp_exp_payment,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_excl AS mapcecl
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcecl.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcecl.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_exclusion AS cexl ON mapcecl.ce_exclusion_id = cexl.id
      AND mapcecl.schema_id = cexl.schema_id
      WHERE upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE 'IMPLANT%'
        OR upper(cexl.description) LIKE '%IMPLANT'
        OR upper(cexl.description) LIKE '%IMPLANT%'
      GROUP BY 2,
               3) AS imp_exp ON imp_exp.mon_account_payer_id = clmax.mon_account_payer_id
   AND imp_exp.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS passthrough_amount,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ce ON mapcsvc.ce_service_id = ce.id
      AND mapcsvc.schema_id = ce.schema_id
      WHERE upper(ce.name) LIKE '%PASS THROUGH%'
        OR upper(ce.name) LIKE 'PX-T%'
      GROUP BY 2,
               3) AS pass ON pass.mon_account_payer_id = clmax.mon_account_payer_id
   AND pass.schema_id = clmax.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS not_ect_amount,
             mapcl.mon_account_payer_id,
             mapcl.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ce ON mapcsvc.ce_service_id = ce.id
      AND mapcsvc.schema_id = ce.schema_id
      WHERE upper(ce.name) NOT LIKE '%ECT%'
        AND mapcsvc.ce_rule_type_id IN(4,
                                       40,
                                       79)
      GROUP BY 2,
               3) AS ect ON ect.mon_account_payer_id = clmax.mon_account_payer_id
   AND ect.schema_id = clmax.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_irf AS mapcirf ON mpcl.id = mapcirf.mon_acct_payer_calc_summary_id
   AND mpcl.schema_id = mapcirf.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_irf_profile AS cirf ON mapcirf.ce_irf_profile_id = cirf.id
   AND mapcirf.schema_id = cirf.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.code_versions AS cdv ON mpcl.drg_version = cdv.id
   AND mpcl.schema_id = cdv.schema_id
   LEFT OUTER JOIN (select * from {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period ) as calc_fap ON clmax.calc_date BETWEEN calc_fap.start_date AND calc_fap.end_date
   AND clmax.schema_id = calc_fap.schema_id
   LEFT OUTER JOIN  (select * from {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period ) AS calcfirstfive_fap ON clmax.calc_date_1 BETWEEN calcfirstfive_fap.start_date AND calcfirstfive_fap.end_date
   AND clmax.schema_id = calcfirstfive_fap.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(trim(a.coid)) = upper(trim(reforg.coid))
   AND upper(trim(a.company_code)) = upper(trim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) AS FLOAT64)
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_cost_period AS ma_13 ON CASE
                                                                                                       WHEN upper(trim(ma.billing_status)) = 'B' THEN CASE
                                                                                                                                                           WHEN extract(DAY
                                                                                                                                                                        FROM clmax.calc_date) BETWEEN 1 AND 5 THEN CASE
                                                                                                                                                                                                                       WHEN ma.mapd_id < calc_fap.id THEN calcfirstfive_fap.id
                                                                                                                                                                                                                       WHEN ma.mapd_id >= calc_fap.id THEN ma.mapd_id
                                                                                                                                                                                                                   END
                                                                                                                                                           ELSE CASE
                                                                                                                                                                    WHEN clmax.calc_date BETWEEN ma.start_date AND ma.end_date THEN ma.mapd_id
                                                                                                                                                                    WHEN CAST(clmax.calc_date AS DATETIME) BETWEEN CAST(date_add(ma.end_date, interval 1 DAY) AS DATETIME) AND CAST(calc_fap.close_date AS DATETIME) + INTERVAL 12 HOUR THEN CASE
                                                                                                                                                                                                                                                                                                                                                 WHEN ma.mapd_id <= calc_fap.id THEN calc_fap.id
                                                                                                                                                                                                                                                                                                                                                 WHEN ma.mapd_id > calc_fap.id THEN ma.mapd_id
                                                                                                                                                                                                                                                                                                                                             END
                                                                                                                                                                END
                                                                                                                                                       END
                                                                                                   END = ma_13.mon_accounting_period_id
   AND ma.org_id_provider = ma_13.cost_report_org_id
   AND ma.schema_id = ma_13.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(trim(pyro.coid)) = upper(trim(reforg.coid))
   AND upper(trim(pyro.company_code)) = upper(trim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   LEFT OUTER JOIN
     (SELECT racp.schema_id,
             racp.mon_account_payer_id,
             1 AS remit_received_flag,
             sum(CASE
                     WHEN ca.ra_category_id = 510 THEN ca.amount
                     ELSE CAST(0 AS NUMERIC)
                 END) AS ra_coinsurance,
             sum(CASE
                     WHEN ca.ra_category_id = 550 THEN ca.amount
                     ELSE CAST(0 AS NUMERIC)
                 END) AS ra_deductible,
             sum(CASE
                     WHEN ca.ra_category_id = 520 THEN ca.amount
                     ELSE CAST(0 AS NUMERIC)
                 END) AS ra_copay,
             sum(CASE
                     WHEN ca.ra_category_id = 570 THEN ca.amount
                     ELSE CAST(0 AS NUMERIC)
                 END) AS ra_pt_resp_non_covered_amt
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated AS ca ON ca.ra_claim_payment_id = racp.id
      AND ca.schema_id = racp.schema_id
      WHERE racp.is_deleted <> 1
      GROUP BY 1,
               2) AS rmt ON rmt.mon_account_payer_id = mapyr.id
   AND rmt.schema_id = mapyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile AS crpf ON mpcl.cers_profile_id = crpf.id
   AND mpcl.schema_id = crpf.schema_id
   CROSS JOIN UNNEST(ARRAY[ CAST(clmax.payer_rank AS INT64) ]) AS iplan_insurance_order_num
   CROSS JOIN UNNEST(ARRAY[ reforg.coid ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
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
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.eor_pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.eor_pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.eor_iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.eor_iplan_id, 1))
AND (coalesce(mt.financial_class_code, NUMERIC '0') = coalesce(ms.financial_class_code, NUMERIC '0')
     AND coalesce(mt.financial_class_code, NUMERIC '1') = coalesce(ms.financial_class_code, NUMERIC '1'))
AND (coalesce(mt.final_bill_date, DATE '1970-01-01') = coalesce(ms.final_bill_date, DATE '1970-01-01')
     AND coalesce(mt.final_bill_date, DATE '1970-01-02') = coalesce(ms.final_bill_date, DATE '1970-01-02'))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (coalesce(mt.ar_bill_drop_date, DATE '1970-01-01') = coalesce(ms.ar_bill_drop_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_drop_date, DATE '1970-01-02') = coalesce(ms.ar_bill_drop_date, DATE '1970-01-02'))
AND (coalesce(mt.contract_begin_date, DATE '1970-01-01') = coalesce(ms.contract_begin_date, DATE '1970-01-01')
     AND coalesce(mt.contract_begin_date, DATE '1970-01-02') = coalesce(ms.contract_begin_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.bill_reason_code, '0')) = upper(coalesce(ms.bill_reason_code, '0'))
     AND upper(coalesce(mt.bill_reason_code, '1')) = upper(coalesce(ms.bill_reason_code, '1')))
AND (coalesce(mt.eor_transaction_date, DATE '1970-01-01') = coalesce(ms.eor_transaction_date, DATE '1970-01-01')
     AND coalesce(mt.eor_transaction_date, DATE '1970-01-02') = coalesce(ms.eor_transaction_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.eor_drg_code, '0')) = upper(coalesce(ms.eor_drg_code, '0'))
     AND upper(coalesce(mt.eor_drg_code, '1')) = upper(coalesce(ms.eor_drg_code, '1')))
AND (upper(coalesce(mt.cc_drg_code, '0')) = upper(coalesce(ms.cc_drg_code, '0'))
     AND upper(coalesce(mt.cc_drg_code, '1')) = upper(coalesce(ms.cc_drg_code, '1')))
AND (coalesce(mt.cc_drg_weight, NUMERIC '0') = coalesce(ms.cc_drg_weight, NUMERIC '0')
     AND coalesce(mt.cc_drg_weight, NUMERIC '1') = coalesce(ms.cc_drg_weight, NUMERIC '1'))
AND (upper(coalesce(mt.eor_hipps_code, '0')) = upper(coalesce(ms.eor_hipps_code, '0'))
     AND upper(coalesce(mt.eor_hipps_code, '1')) = upper(coalesce(ms.eor_hipps_code, '1')))
AND (upper(coalesce(mt.cc_cmg_code, '0')) = upper(coalesce(ms.cc_cmg_code, '0'))
     AND upper(coalesce(mt.cc_cmg_code, '1')) = upper(coalesce(ms.cc_cmg_code, '1')))
AND (coalesce(mt.cc_cmg_weight, NUMERIC '0') = coalesce(ms.cc_cmg_weight, NUMERIC '0')
     AND coalesce(mt.cc_cmg_weight, NUMERIC '1') = coalesce(ms.cc_cmg_weight, NUMERIC '1'))
AND (coalesce(mt.eor_covered_days_num, 0) = coalesce(ms.eor_covered_days_num, 0)
     AND coalesce(mt.eor_covered_days_num, 1) = coalesce(ms.eor_covered_days_num, 1))
AND (coalesce(mt.eor_total_charge_amt, NUMERIC '0') = coalesce(ms.eor_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_total_charge_amt, NUMERIC '1') = coalesce(ms.eor_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.eor_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.eor_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.eor_cmptd_reimbursement_amt, NUMERIC '0') = coalesce(ms.eor_cmptd_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.eor_cmptd_reimbursement_amt, NUMERIC '1') = coalesce(ms.eor_cmptd_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.eor_deductible_amt, NUMERIC '0') = coalesce(ms.eor_deductible_amt, NUMERIC '0')
     AND coalesce(mt.eor_deductible_amt, NUMERIC '1') = coalesce(ms.eor_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.eor_coinsurance_amt, NUMERIC '0') = coalesce(ms.eor_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.eor_coinsurance_amt, NUMERIC '1') = coalesce(ms.eor_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_copay_amt, NUMERIC '0') = coalesce(ms.eor_copay_amt, NUMERIC '0')
     AND coalesce(mt.eor_copay_amt, NUMERIC '1') = coalesce(ms.eor_copay_amt, NUMERIC '1'))
AND (coalesce(mt.pat_resp_non_covered_chg_amt, NUMERIC '0') = coalesce(ms.pat_resp_non_covered_chg_amt, NUMERIC '0')
     AND coalesce(mt.pat_resp_non_covered_chg_amt, NUMERIC '1') = coalesce(ms.pat_resp_non_covered_chg_amt, NUMERIC '1'))
AND (coalesce(mt.expected_insurance_payment_amt, NUMERIC '0') = coalesce(ms.expected_insurance_payment_amt, NUMERIC '0')
     AND coalesce(mt.expected_insurance_payment_amt, NUMERIC '1') = coalesce(ms.expected_insurance_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_actual_pat_resp_amt, NUMERIC '0') = coalesce(ms.total_actual_pat_resp_amt, NUMERIC '0')
     AND coalesce(mt.total_actual_pat_resp_amt, NUMERIC '1') = coalesce(ms.total_actual_pat_resp_amt, NUMERIC '1'))
AND (coalesce(mt.eor_insurance_payment_amt, NUMERIC '0') = coalesce(ms.eor_insurance_payment_amt, NUMERIC '0')
     AND coalesce(mt.eor_insurance_payment_amt, NUMERIC '1') = coalesce(ms.eor_insurance_payment_amt, NUMERIC '1'))
AND (coalesce(mt.eor_contractual_allowance_amt, NUMERIC '0') = coalesce(ms.eor_contractual_allowance_amt, NUMERIC '0')
     AND coalesce(mt.eor_contractual_allowance_amt, NUMERIC '1') = coalesce(ms.eor_contractual_allowance_amt, NUMERIC '1'))
AND (coalesce(mt.eor_cost_of_service_amt, NUMERIC '0') = coalesce(ms.eor_cost_of_service_amt, NUMERIC '0')
     AND coalesce(mt.eor_cost_of_service_amt, NUMERIC '1') = coalesce(ms.eor_cost_of_service_amt, NUMERIC '1'))
AND (upper(coalesce(mt.month_13_ind, '0')) = upper(coalesce(ms.month_13_ind, '0'))
     AND upper(coalesce(mt.month_13_ind, '1')) = upper(coalesce(ms.month_13_ind, '1')))
AND (upper(coalesce(mt.outlier_code, '0')) = upper(coalesce(ms.outlier_code, '0'))
     AND upper(coalesce(mt.outlier_code, '1')) = upper(coalesce(ms.outlier_code, '1')))
AND (coalesce(mt.auto_post_amt, NUMERIC '0') = coalesce(ms.auto_post_amt, NUMERIC '0')
     AND coalesce(mt.auto_post_amt, NUMERIC '1') = coalesce(ms.auto_post_amt, NUMERIC '1'))
AND (upper(coalesce(mt.auto_post_ind, '0')) = upper(coalesce(ms.auto_post_ind, '0'))
     AND upper(coalesce(mt.auto_post_ind, '1')) = upper(coalesce(ms.auto_post_ind, '1')))
AND (upper(coalesce(mt.drg_table_id, '0')) = upper(coalesce(ms.drg_table_id, '0'))
     AND upper(coalesce(mt.drg_table_id, '1')) = upper(coalesce(ms.drg_table_id, '1')))
AND (upper(coalesce(mt.opps_ind, '0')) = upper(coalesce(ms.opps_ind, '0'))
     AND upper(coalesce(mt.opps_ind, '1')) = upper(coalesce(ms.opps_ind, '1')))
AND (upper(coalesce(mt.inpatient_outpatient_ind, '0')) = upper(coalesce(ms.inpatient_outpatient_ind, '0'))
     AND upper(coalesce(mt.inpatient_outpatient_ind, '1')) = upper(coalesce(ms.inpatient_outpatient_ind, '1')))
AND (coalesce(mt.visit_count, 0) = coalesce(ms.visit_count, 0)
     AND coalesce(mt.visit_count, 1) = coalesce(ms.visit_count, 1))
AND (coalesce(mt.eor_gross_reimbursement_amt, NUMERIC '0') = coalesce(ms.eor_gross_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.eor_gross_reimbursement_amt, NUMERIC '1') = coalesce(ms.eor_gross_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.eor_net_reimbursement_amt, NUMERIC '0') = coalesce(ms.eor_net_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.eor_net_reimbursement_amt, NUMERIC '1') = coalesce(ms.eor_net_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.eor_non_bill_charge_amt, NUMERIC '0') = coalesce(ms.eor_non_bill_charge_amt, NUMERIC '0')
     AND coalesce(mt.eor_non_bill_charge_amt, NUMERIC '1') = coalesce(ms.eor_non_bill_charge_amt, NUMERIC '1'))
AND (coalesce(mt.sqstrtn_red_amt, NUMERIC '0') = coalesce(ms.sqstrtn_red_amt, NUMERIC '0')
     AND coalesce(mt.sqstrtn_red_amt, NUMERIC '1') = coalesce(ms.sqstrtn_red_amt, NUMERIC '1'))
AND (upper(coalesce(mt.model_name, '0')) = upper(coalesce(ms.model_name, '0'))
     AND upper(coalesce(mt.model_name, '1')) = upper(coalesce(ms.model_name, '1')))
AND (upper(coalesce(mt.project_desc, '0')) = upper(coalesce(ms.project_desc, '0'))
     AND upper(coalesce(mt.project_desc, '1')) = upper(coalesce(ms.project_desc, '1')))
AND (coalesce(mt.eor_ipf_interrupted_day_stay, NUMERIC '0') = coalesce(ms.eor_ipf_interrupted_day_stay, NUMERIC '0')
     AND coalesce(mt.eor_ipf_interrupted_day_stay, NUMERIC '1') = coalesce(ms.eor_ipf_interrupted_day_stay, NUMERIC '1'))
AND (upper(coalesce(mt.calc_success_ind, '0')) = upper(coalesce(ms.calc_success_ind, '0'))
     AND upper(coalesce(mt.calc_success_ind, '1')) = upper(coalesce(ms.calc_success_ind, '1')))
AND (upper(coalesce(mt.latest_calc_ind, '0')) = upper(coalesce(ms.latest_calc_ind, '0'))
     AND upper(coalesce(mt.latest_calc_ind, '1')) = upper(coalesce(ms.latest_calc_ind, '1')))
AND (upper(coalesce(mt.interim_bill_ind, '0')) = upper(coalesce(ms.interim_bill_ind, '0'))
     AND upper(coalesce(mt.interim_bill_ind, '1')) = upper(coalesce(ms.interim_bill_ind, '1')))
AND (coalesce(mt.financial_period_id, NUMERIC '0') = coalesce(ms.financial_period_id, NUMERIC '0')
     AND coalesce(mt.financial_period_id, NUMERIC '1') = coalesce(ms.financial_period_id, NUMERIC '1'))
AND (coalesce(mt.reason_id, NUMERIC '0') = coalesce(ms.reason_id, NUMERIC '0')
     AND coalesce(mt.reason_id, NUMERIC '1') = coalesce(ms.reason_id, NUMERIC '1'))
AND (coalesce(mt.calc_id, NUMERIC '0') = coalesce(ms.calc_id, NUMERIC '0')
     AND coalesce(mt.calc_id, NUMERIC '1') = coalesce(ms.calc_id, NUMERIC '1'))
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
        financial_class_code,
        final_bill_date,
        ar_bill_thru_date,
        ar_bill_drop_date,
        contract_begin_date,
        bill_reason_code,
        eor_transaction_date,
        eor_drg_code,
        cc_drg_code,
        cc_drg_weight,
        eor_hipps_code,
        cc_cmg_code,
        cc_cmg_weight,
        eor_covered_days_num,
        eor_total_charge_amt,
        eor_non_covered_charge_amt,
        eor_cmptd_reimbursement_amt,
        eor_deductible_amt,
        eor_coinsurance_amt,
        eor_copay_amt,
        pat_resp_non_covered_chg_amt,
        expected_insurance_payment_amt,
        total_actual_pat_resp_amt,
        eor_insurance_payment_amt,
        eor_contractual_allowance_amt,
        eor_cost_of_service_amt,
        month_13_ind,
        outlier_code,
        auto_post_amt,
        auto_post_ind,
        drg_table_id,
        opps_ind,
        inpatient_outpatient_ind,
        visit_count,
        eor_gross_reimbursement_amt,
        eor_net_reimbursement_amt,
        eor_non_bill_charge_amt,
        sqstrtn_red_amt,
        model_name,
        project_desc,
        eor_ipf_interrupted_day_stay,
        calc_success_ind,
        latest_calc_ind,
        interim_bill_ind,
        financial_period_id,
        reason_id,
        calc_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_cd, ms.coido, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.unit_num, ms.eor_pat_acct_num, ms.eor_iplan_id, ms.financial_class_code, ms.final_bill_date, ms.ar_bill_thru_date, ms.ar_bill_drop_date, ms.contract_begin_date, ms.bill_reason_code, ms.eor_transaction_date, ms.eor_drg_code, ms.cc_drg_code, ms.cc_drg_weight, ms.eor_hipps_code, ms.cc_cmg_code, ms.cc_cmg_weight, ms.eor_covered_days_num, ms.eor_total_charge_amt, ms.eor_non_covered_charge_amt, ms.eor_cmptd_reimbursement_amt, ms.eor_deductible_amt, ms.eor_coinsurance_amt, ms.eor_copay_amt, ms.pat_resp_non_covered_chg_amt, ms.expected_insurance_payment_amt, ms.total_actual_pat_resp_amt, ms.eor_insurance_payment_amt, ms.eor_contractual_allowance_amt, ms.eor_cost_of_service_amt, ms.month_13_ind, ms.outlier_code, ms.auto_post_amt, ms.auto_post_ind, ms.drg_table_id, ms.opps_ind, ms.inpatient_outpatient_ind, ms.visit_count, ms.eor_gross_reimbursement_amt, ms.eor_net_reimbursement_amt, ms.eor_non_bill_charge_amt, ms.sqstrtn_red_amt, ms.model_name, ms.project_desc, ms.eor_ipf_interrupted_day_stay, ms.calc_success_ind, ms.latest_calc_ind, ms.interim_bill_ind, ms.financial_period_id, ms.reason_id, ms.calc_id, ms.dw_last_update_date_time, ms.source_system_code);


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
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_stg
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_EOR_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor AS x USING
  (SELECT cc_eor_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_stg) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_code))
AND upper(trim(x.coid)) = upper(trim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.eor_log_date = z.eor_log_date
AND upper(trim(x.log_id)) = upper(trim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND x.eff_from_date = z.eff_from_date WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    financial_class_code = z.financial_class_code,
    final_bill_date = z.final_bill_date,
    ar_bill_thru_date = z.ar_bill_thru_date,
    ar_bill_drop_date = z.ar_bill_drop_date,
    contract_begin_date = z.contract_begin_date,
    bill_reason_code = z.bill_reason_code,
    eor_transaction_date = z.eor_transaction_date,
    eor_drg_code = z.eor_drg_code,
    cc_drg_code = z.cc_drg_code,
    cc_drg_weight = z.cc_drg_weight,
    eor_hipps_code = z.eor_hipps_code,
    cc_cmg_code = z.cc_cmg_code,
    cc_cmg_weight = z.cc_cmg_weight,
    eor_covered_days_num = z.eor_covered_days_num,
    eor_total_charge_amt = z.eor_total_charge_amt,
    eor_non_covered_charge_amt = z.eor_non_covered_charge_amt,
    eor_cmptd_reimbursement_amt = z.eor_cmptd_reimbursement_amt,
    eor_deductible_amt = z.eor_deductible_amt,
    eor_coinsurance_amt = z.eor_coinsurance_amt,
    eor_copay_amt = z.eor_copay_amt,
    pat_resp_non_covered_chg_amt = z.pat_resp_non_covered_chg_amt,
    expected_insurance_payment_amt = z.expected_insurance_payment_amt,
    total_actual_pat_resp_amt = z.total_actual_pat_resp_amt,
    eor_insurance_payment_amt = z.eor_insurance_payment_amt,
    eor_contractual_allowance_amt = z.eor_contractual_allowance_amt,
    eor_cost_of_service_amt = z.eor_cost_of_service_amt,
    month_13_ind = z.month_13_ind,
    outlier_code = z.outlier_code,
    auto_post_amt = z.auto_post_amt,
    auto_post_ind = z.auto_post_ind,
    drg_table_id = z.drg_table_id,
    opps_ind = z.opps_ind,
    inpatient_outpatient_ind = z.inpatient_outpatient_ind,
    visit_count = z.visit_count,
    eor_gross_reimbursement_amt = z.eor_gross_reimbursement_amt,
    eor_net_reimbursement_amt = z.eor_net_reimbursement_amt,
    eor_non_bill_charge_amt = z.eor_non_bill_charge_amt,
    sqstrtn_red_amt = z.sqstrtn_red_amt,
    model_name = z.model_name,
    project_desc = z.project_desc,
    eor_ipf_interrupted_day_stay = z.eor_ipf_interrupted_day_stay,
    calc_success_ind = z.calc_success_ind,
    latest_calc_ind = z.latest_calc_ind,
    interim_bill_ind = z.interim_bill_ind,
    financial_period_id = z.financial_period_id,
    reason_id = z.reason_id,
    calc_id = z.calc_id,
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
        financial_class_code,
        final_bill_date,
        ar_bill_thru_date,
        ar_bill_drop_date,
        contract_begin_date,
        bill_reason_code,
        eor_transaction_date,
        eor_drg_code,
        cc_drg_code,
        cc_drg_weight,
        eor_hipps_code,
        cc_cmg_code,
        cc_cmg_weight,
        eor_covered_days_num,
        eor_total_charge_amt,
        eor_non_covered_charge_amt,
        eor_cmptd_reimbursement_amt,
        eor_deductible_amt,
        eor_coinsurance_amt,
        eor_copay_amt,
        pat_resp_non_covered_chg_amt,
        expected_insurance_payment_amt,
        total_actual_pat_resp_amt,
        eor_insurance_payment_amt,
        eor_contractual_allowance_amt,
        eor_cost_of_service_amt,
        month_13_ind,
        outlier_code,
        auto_post_amt,
        auto_post_ind,
        drg_table_id,
        opps_ind,
        inpatient_outpatient_ind,
        visit_count,
        eor_gross_reimbursement_amt,
        eor_net_reimbursement_amt,
        eor_non_bill_charge_amt,
        sqstrtn_red_amt,
        model_name,
        project_desc,
        eor_ipf_interrupted_day_stay,
        calc_success_ind,
        latest_calc_ind,
        interim_bill_ind,
        financial_period_id,
        reason_id,
        calc_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.iplan_insurance_order_num, z.eor_log_date, z.log_id, z.log_sequence_num, z.eff_from_date, z.unit_num, z.pat_acct_num, z.iplan_id, z.financial_class_code, z.final_bill_date, z.ar_bill_thru_date, z.ar_bill_drop_date, z.contract_begin_date, z.bill_reason_code, z.eor_transaction_date, z.eor_drg_code, z.cc_drg_code, z.cc_drg_weight, z.eor_hipps_code, z.cc_cmg_code, z.cc_cmg_weight, z.eor_covered_days_num, z.eor_total_charge_amt, z.eor_non_covered_charge_amt, z.eor_cmptd_reimbursement_amt, z.eor_deductible_amt, z.eor_coinsurance_amt, z.eor_copay_amt, z.pat_resp_non_covered_chg_amt, z.expected_insurance_payment_amt, z.total_actual_pat_resp_amt, z.eor_insurance_payment_amt, z.eor_contractual_allowance_amt, z.eor_cost_of_service_amt, z.month_13_ind, z.outlier_code, z.auto_post_amt, z.auto_post_ind, z.drg_table_id, z.opps_ind, z.inpatient_outpatient_ind, z.visit_count, z.eor_gross_reimbursement_amt, z.eor_net_reimbursement_amt, z.eor_non_bill_charge_amt, z.sqstrtn_red_amt, z.model_name, z.project_desc, z.eor_ipf_interrupted_day_stay, z.calc_success_ind, z.latest_calc_ind, z.interim_bill_ind, z.financial_period_id, z.reason_id, z.calc_id, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


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
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor as cc_eor
WHERE upper(cc_eor.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(trim(r.org_status)) = 'ACTIVE' )
  AND cc_eor.dw_last_update_date_time <>
    (SELECT max(cc_eor_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor AS cc_eor_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_EOR');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;