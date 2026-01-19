DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_amount_stg_calc_service.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Ashan
       Date: 02/03/2014
       Name: EOR_Amount_Stg_Calc_Service.sql
       Mod1: Added Seq & DSH Care Uncmps Care Addon Amt. 2/18/2014. AP.
       Mod2: Added Hac_Adj_Amt. 10/01/2014. AP.
       Mod3: Added RA_Remit_Received_Flag,RA_Deductable,RA_Coinsurance,RA_Copay,RA_Pat_Resp_Non_Covered_Am. 10/09/2014 .AP.
       Mod4: Measures from MAPCL table are changed to sum the multiple calcs. It might affect category code 30 and 86. 10/22/2014. AP.
             Mapcl_Tot_Exp_Payment,Mapcl_Base_Exp_Payment,Mapcl_Exclusion_Exp_Payment,Mapcl_Stoploss_Exp_Payment are update to sum multiple calcs. 10/22/2104. AP.
       Mod5: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	Mod6:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod7: Addded single quote at Mapyr.Misc_Char01 = '1' for improved performance to avoid internal conversion. PT 12/5/2018
	Mod8: Column Calc_Fee_schedule_Adj added as part of TRICARE and MCAREIRF code changes.Modified to use name column from cers_profile instead of rate_schedule_name from Mon_Account_Payer .
	Added trim function to crpf.name column to have all valid records,refer PBI 18374 and PBI 20158   PT 1/4/2019
	Mod9: Added more like any strings as per PBI19554 AM 2/10/2019
	Mod10: Added trim for ce.name and added more strings in like for calculating columnbased onlab charges, ECT,HCD etc based on name PBI19554 AM 2/14/2019
	Mod11: Added logic for MIPS calculation. as part of this added below new columns :ACCT_SUBTERM_AMT,GVT_DEVICE_OFFSET_REDUCTION, GVT_SCP_PAYMENT
  ,VAL_BASED_ADJMT_AMT,MDH_ADD_ON_AMT,READM_ADJMT_AMT,LOW_VOL_ADD_ON_AMT,COB_AMOUNT
   ,PASSTHROUGH_AMOUNT,GVT_Operating_Outlier_Pmt AM 4/5/2019
Mod12:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA147_CALC_SERVICE;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- -Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS mt USING
  (SELECT DISTINCT reforg.company_code AS company_code,
                   reforg.coid AS coid,
                   a.patient_dw_id AS patient_dw_id,
                   pyro.payor_dw_id AS payor_dw_id,
                   iplan_insurance_order_num AS iplan_insurance_order_num,
                   clmax.calculation_date AS eor_log_date,
                   substr(concat('INS', trim(format('%11d', CAST(mapyr.payer_rank AS INT64)))), 1, 14) AS log_id,
                   row_number() OVER (PARTITION BY upper(reforg.company_code),
                                                   upper(reforg.coid),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id,
                                                   upper(iplan_insurance_order_num),
                                                   clmax.calculation_date,
                                                   upper(pyro.log_id),
                                                   pyro.eff_from_date
                                      ORDER BY mapcl.id) AS log_sequence_num,
                                     CASE
                                         WHEN clmax.calculation_date IS NULL THEN DATE '1800-01-01'
                                         ELSE DATE(clmax.calculation_date)
                                     END AS eff_from_date,
                                     substr(CASE
                                                WHEN clmax.drg IS NOT NULL THEN 'D'
                                                WHEN upper(rtrim(mapyr.misc_yn01)) = 'Y' THEN 'O'
                                                ELSE 'T'
                                            END, 1, 2) AS reimbursement_method_type_code,
                                     reforg.unit_num AS unit_num,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS eor_pat_acct_num,
                                     CASE
                                         WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                                     END AS eor_iplan_id,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                     'N' AS source_system_code,
                                     substr(CASE
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIPS IP%' THEN 'MIPS'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV IPBO%' THEN 'IPBO'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIPF%' THEN 'MIPF'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MIRF%' THEN 'MIRF'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MSNF%'
                                                     OR upper(trim(crpf.name)) LIKE 'GOV MSWB%' THEN 'MSNF'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MOPS OP%'
                                                     OR upper(trim(crpf.name)) LIKE 'GOV MCRO%' THEN 'MOPS'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MEDICAID IP%' THEN 'MCDIP'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV MEDICAID OP%' THEN 'MCDOP'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE IP%' THEN 'TCRIP'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE OP%' THEN 'TCROP'
                                                WHEN upper(trim(crpf.name)) LIKE 'GOV TRICARE IRF%' THEN 'TCRIF'
                                                WHEN upper(trim(crpf.name)) NOT LIKE 'GOV%'
                                                     OR trim(crpf.name) IS NULL THEN 'MCARE'
                                            END, 1, 10) AS rate_schedule,
                                     mapyr.mon_account_id AS mon_account_id,
                                     mapyr.mon_payer_id AS mon_payer_id,
                                     mapcl.schema_id AS schema_id,
                                     mapyr.id AS mon_account_payer_id,
                                     ROUND(coalesce(mapyr.misc_amt02, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_misc_amt02,
                                     ROUND(coalesce(mapyr.misc_amt01, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_misc_amt01,
                                     ROUND(coalesce(mapyr.copay, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_copay,
                                     ROUND(coalesce(mapyr.coinsurance, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_coinsurance,
                                     ROUND(coalesce(mapyr.deductible, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_deductible,
                                     ROUND(coalesce(mapyr.covered_charges_inst, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_covered_charges,
                                     ROUND(coalesce(mapyr.total_payments, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_total_payments,
                                     ROUND(coalesce(mapyr.total_variance_adjustment, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_total_variance_adj,
                                     ROUND(coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS map_total_exp_payment,
                                     coalesce(mapyr.state_tax_amt, CAST(0 AS NUMERIC)) AS map_sequestration_amt,
                                     CAST(ROUND(coalesce(clmax.total_expected_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS mapcl_tot_exp_payment,
                                     CAST(ROUND(coalesce(clmax.base_expected_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS mapcl_base_exp_payment,
                                     CAST(ROUND(coalesce(clmax.exclusion_expected_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS mapcl_exclusion_exp_payment,
                                     CAST(ROUND(coalesce(clmax.stoploss_expected_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS mapcl_stoploss_exp_payment,
                                     ROUND(coalesce(ma.total_charges, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS ma_total_charges,
                                     ROUND(coalesce(ma.billed_charges, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS ma_billed_charges,
                                     ROUND(coalesce(ma.misc_amt01, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') AS ma_misc_amt01,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_capital_drg_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_capital_drg_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_capital_outlier_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_capital_outlier_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_operating_fed_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_operating_fed_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_ime_operating_drg_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_ime_operating_drg_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_outlier_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_outlier_payment,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_dsh_operating_drg_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_dsh_operating_drg_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_capital_cost_outlier, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_capital_cost_outlier,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_newtech_add_on, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_newtech_add_on,
                                     CAST(ROUND(coalesce(svc_gvt.tot_expected_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS tot_expected_payment,
                                     CAST(ROUND(coalesce(svc_gvt.noncovered_charges, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS noncovered_charges,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_drg_pmt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_drg_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_cost_threshold, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_cost_threshold,
                                     CAST(ROUND(coalesce(CASE
                                                             WHEN upper(rtrim(mapyr.misc_char01)) = '1'
                                                                  AND mapyr.payer_rank = 1
                                                                  AND upper(rtrim(mpt.ip_op_ind)) = 'I' THEN svc_gvt.gvt_capital_cost
                                                         END, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ip_gvt_capital_cost,
                                     CAST(ROUND(coalesce(CASE
                                                             WHEN upper(rtrim(mapyr.misc_char01)) = '1'
                                                                  AND mapyr.payer_rank = 1
                                                                  AND upper(rtrim(mpt.ip_op_ind)) = 'I' THEN svc_gvt.gvt_dsh_capital_drg_pmt
                                                         END, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ip_gvt_dsh_capital_drg_pmt,
                                     CAST(ROUND(coalesce(CASE
                                                             WHEN upper(rtrim(mapyr.misc_char01)) = '1'
                                                                  AND mapyr.payer_rank = 1
                                                                  AND upper(rtrim(mpt.ip_op_ind)) = 'I' THEN svc_gvt.gvt_adj_capital_fed_pmt
                                                         END, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ip_gvt_adj_capital_fed_pmt,
                                     CAST(ROUND(coalesce(svc_gvt.dsh_uncmps_care_addon_amt, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS dsh_uncmps_care_addon_amt,
                                     CAST(ROUND(coalesce(svc_gvt.ipf_pps_out_per_diem_add_on, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ipf_pps_out_per_diem_add_on,
                                     CAST(ROUND(coalesce(svc_gvt.ipf_pps_adj_per_diem, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ipf_pps_adj_per_diem,
                                     CAST(coalesce(svc_gvt.quantity, CAST(0 AS BIGNUMERIC)) AS INT64) AS quantity,
                                     CAST(ROUND(coalesce(svc_gvt.comp_flg_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS comp_flg_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.comp_flg_apc_outlier_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS comp_flg_apc_outlier_amount,
                                     CAST(ROUND(coalesce(svc_gvt.comp_flg_apc_coinsurance_amt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS comp_flg_apc_coinsurance_amt,
                                     CAST(ROUND(coalesce(svc_gvt.comp_flg_apc_deductible_amt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS comp_flg_apc_deductible_amt,
                                     CAST(ROUND(coalesce(svc_gvt.srv_mcrtherapy_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mcrtherapy_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_hcd_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_hcd_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_mri_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mri_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_mri_hcd_imp_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mri_hcd_imp_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_imp_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_imp_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_lab_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_lab_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_lab_service_charges, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_lab_service_charges,
                                     CAST(ROUND(coalesce(svc_gvt.srv_mcrlab_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mcrlab_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_tricmac_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_tricmac_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_ect_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_ect_exp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.srv_mcrlabtherapy_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS srv_mcrlabtherapy_exp_payment,
                                     CAST(ROUND(coalesce(calc_atp.calc_atp_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_atp_exp_payment,
                                     CAST(ROUND(coalesce(calc_atp.calc_atp_charge_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_atp_charge_amount,
                                     CAST(ROUND(coalesce(calc_apc.calc_apc_outlier_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_apc_outlier_payment,
                                     CAST(ROUND(coalesce(calc_apc.calc_apc_exp_payment, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_apc_exp_payment,
                                     CAST(ROUND(coalesce(calc_apc.calc_apc_deductible_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_apc_deductible_amount,
                                     CAST(ROUND(coalesce(calc_apc.calc_apc_coinsurance_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_apc_coinsurance_amount,
                                     coalesce(loa.loa_count, 0) AS loa_count,
                                     CAST(ROUND(coalesce(svc_gvt.hac_adjmt_amt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS hac_adjmt_amt,
                                     coalesce(rmt.remit_received_flag, 0) AS remit_received_flag,
                                     CAST(ROUND(coalesce(rmt.ra_deductible, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_deductible,
                                     CAST(ROUND(coalesce(rmt.ra_coinsurance, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_coinsurance,
                                     CAST(ROUND(coalesce(rmt.ra_copay, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_copay,
                                     CAST(ROUND(coalesce(rmt.ra_pt_resp_non_covered_amt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_pt_resp_non_covered_amt,
                                     CAST(ROUND(coalesce(svc_gvt.calc_fee_schedule_adj, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS calc_fee_schedule_adj,
                                     ROUND(coalesce(mapcl.acct_subterm_amt, CAST(0 AS NUMERIC)), 2, 'ROUND_HALF_EVEN') AS acct_subterm_amt,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_device_offset_reduction, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_device_offset_reduction,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_scp_payment, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_scp_payment,
                                     CAST(ROUND(coalesce(svc_gvt.val_based_adjmt_amt, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS val_based_adjmt_amt,
                                     CAST(ROUND(coalesce(svc_gvt.mdh_add_on_amt, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS mdh_add_on_amt,
                                     CAST(ROUND(coalesce(svc_gvt.readm_adjmt_amt, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS readm_adjmt_amt,
                                     CAST(ROUND(coalesce(svc_gvt.low_vol_add_on_amt, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS low_vol_add_on_amt,
                                     ROUND(CASE
                                               WHEN mapyr.payer_rank = 2
                                                    AND upper(rtrim(mapcl.cc_result)) = 'SUCCESS'
                                                    AND mapcl.cob_method_id IS NOT NULL
                                                    AND mapcl.cob_method_id <> 2666602 THEN coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(mapyr.orig_rank_2_expected_payment, CAST(0 AS NUMERIC))
                                               ELSE CAST(0 AS NUMERIC)
                                           END, 2, 'ROUND_HALF_EVEN') AS cob_amount,
                                     CAST(ROUND(coalesce(svc_gvt.passthrough_amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS passthrough_amount,
                                     CAST(ROUND(coalesce(svc_gvt.gvt_operating_outlier_pmt, CAST(0 AS BIGNUMERIC)), 6, 'ROUND_HALF_EVEN') AS NUMERIC) AS gvt_operating_outlier_pmt
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr
   INNER JOIN
     (SELECT max(mpcl.id) AS id,
             max(mpcl.calculation_date) AS calculation_date,
             max(mpcl.drg) AS drg,
             sum(coalesce(mpcl.total_expected_payment, CAST(0 AS NUMERIC))) AS total_expected_payment,
             sum(coalesce(mpcl.base_expected_payment, CAST(0 AS NUMERIC))) AS base_expected_payment,
             sum(coalesce(mpcl.exclusion_expected_payment, CAST(0 AS NUMERIC))) AS exclusion_expected_payment,
             sum(coalesce(mpcl.stoploss_expected_payment, CAST(0 AS NUMERIC))) AS stoploss_expected_payment,
             mpcl.payer_rank,
             max(mpcl.account_no) AS account_no,
             mpcl.schema_id,
             mpcl.mon_account_payer_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl
      GROUP BY 8,
               upper(mpcl.account_no),
               10,
               11) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON clmax.id = mapcl.id
   AND clmax.schema_id = mapcl.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapyr.mon_account_id = ma.id
   AND mapyr.schema_id = ma.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON ma.mon_patient_type_id = mpt.id
   AND ma.schema_id = mpt.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON reforg.org_id = ma.org_id_provider
   AND reforg.schema_id = ma.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
   AND mapyr.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(clmax.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
   LEFT OUTER JOIN --  Calc Service
  {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS cert ON mapcl.cers_term_id = cert.id
   AND mapcl.schema_id = cert.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.cers_profile AS crpf ON cert.cers_profile_id = crpf.id
   AND mapcl.schema_id = crpf.schema_id
   LEFT OUTER JOIN
     (SELECT mapcl_0.mon_account_payer_id AS mon_account_payer_id,
             mapcl_0.schema_id AS schema_id,
             sum(coalesce(mapcsvc.gvt_capital_drg_pmt, CAST(0 AS NUMERIC))) AS gvt_capital_drg_pmt,
             sum(coalesce(mapcsvc.gvt_capital_outlier_pmt, CAST(0 AS NUMERIC))) AS gvt_capital_outlier_pmt,
             sum(coalesce(mapcsvc.gvt_operating_outlier_pmt, CAST(0 AS NUMERIC))) AS gvt_operating_outlier_pmt,
             sum(coalesce(mapcsvc.gvt_operating_fed_pmt, CAST(0 AS NUMERIC))) AS gvt_operating_fed_pmt,
             sum(coalesce(mapcsvc.gvt_ime_operating_drg_pmt, CAST(0 AS NUMERIC))) AS gvt_ime_operating_drg_pmt,
             sum(coalesce(mapcsvc.gvt_outlier_payment, CAST(0 AS NUMERIC))) AS gvt_outlier_payment,
             sum(coalesce(mapcsvc.gvt_dsh_operating_drg_pmt, CAST(0 AS NUMERIC))) AS gvt_dsh_operating_drg_pmt,
             sum(coalesce(mapcsvc.gvt_capital_cost_outlier, CAST(0 AS NUMERIC))) AS gvt_capital_cost_outlier,
             sum(coalesce(mapcsvc.gvt_newtech_add_on, CAST(0 AS NUMERIC))) AS gvt_newtech_add_on,
             sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS tot_expected_payment,
             sum(coalesce(mapcsvc.noncovered_charges, CAST(0 AS NUMERIC))) AS noncovered_charges,
             sum(coalesce(mapcsvc.gvt_drg_pmt, CAST(0 AS NUMERIC))) AS gvt_drg_pmt,
             sum(coalesce(mapcsvc.gvt_cost_threshold, CAST(0 AS NUMERIC))) AS gvt_cost_threshold,
             sum(coalesce(mapcsvc.gvt_capital_cost, CAST(0 AS NUMERIC))) AS gvt_capital_cost,
             sum(coalesce(mapcsvc.gvt_dsh_capital_drg_pmt, CAST(0 AS NUMERIC))) AS gvt_dsh_capital_drg_pmt,
             sum(coalesce(mapcsvc.gvt_adj_capital_fed_pmt, CAST(0 AS NUMERIC))) AS gvt_adj_capital_fed_pmt,
             sum(coalesce(mapcsvc.dsh_uncmps_care_addon_amt, CAST(0 AS NUMERIC))) AS dsh_uncmps_care_addon_amt,
             sum(coalesce(mapcsvc.ipf_pps_out_per_diem_add_on * CASE
                                                                    WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                                                                    ELSE CAST(1 AS NUMERIC)
                                                                END, CAST(0 AS NUMERIC))) AS ipf_pps_out_per_diem_add_on,
             sum(coalesce(mapcsvc.ipf_pps_adj_per_diem * CASE
                                                             WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                                                             ELSE CAST(1 AS NUMERIC)
                                                         END, CAST(0 AS NUMERIC))) AS ipf_pps_adj_per_diem,
             sum(coalesce(CASE
                              WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                              ELSE CAST(1 AS NUMERIC)
                          END, CAST(0 AS NUMERIC))) AS quantity,
             sum(coalesce(CASE
                              WHEN mapcsvc.apc_composite_flag IS NOT NULL
                                   AND mapcsvc.apc_composite_flag <> 0 THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS comp_flg_exp_payment,
             sum(coalesce(CASE
                              WHEN mapcsvc.apc_composite_flag IS NOT NULL
                                   AND mapcsvc.apc_composite_flag <> 0 THEN mapcsvc.apc_outlier_amount
                          END, CAST(0 AS NUMERIC))) AS comp_flg_apc_outlier_amount,
             sum(coalesce(CASE
                              WHEN mapcsvc.apc_composite_flag IS NOT NULL
                                   AND mapcsvc.apc_composite_flag <> 0 THEN mapcsvc.apc_coinsurance_amount
                          END, CAST(0 AS NUMERIC))) AS comp_flg_apc_coinsurance_amt,
             sum(coalesce(CASE
                              WHEN mapcsvc.apc_composite_flag IS NOT NULL
                                   AND mapcsvc.apc_composite_flag <> 0 THEN mapcsvc.apc_deductible_amount
                          END, CAST(0 AS NUMERIC))) AS comp_flg_apc_deductible_amt,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) IN('MEDICARE - THERAPY', 'MEDICARE - THERAPY REDUCTION') THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_mcrtherapy_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'HCD%'
                                   OR upper(trim(ce.name)) LIKE 'HIGH COST DRUGS%'
                                   OR upper(trim(ce.name)) LIKE 'HCD%'
                                   OR upper(trim(ce.name)) LIKE '%HCD'
                                   OR upper(trim(ce.name)) LIKE '%HCD %'
                                   OR upper(trim(ce.name)) LIKE 'HIGH COST DRUGS%'
                                   OR upper(trim(ce.name)) LIKE '%HIGH COST DRUGS%'
                                   OR upper(trim(ce.name)) LIKE 'HIGH COST DRUGS' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_hcd_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'MRI%'
                                   OR upper(trim(ce.name)) LIKE 'CT SCAN%'
                                   OR upper(trim(ce.name)) LIKE 'AMB%'
                                   OR upper(trim(ce.name)) LIKE '%CAT %'
                                   OR upper(trim(ce.name)) LIKE 'CT %'
                                   OR upper(trim(ce.name)) LIKE 'CT/%'
                                   OR upper(trim(ce.name)) LIKE ' CT %'
                                   OR upper(trim(ce.name)) LIKE '%/CT %'
                                   OR upper(trim(ce.name)) LIKE '% CT'
                                   OR upper(trim(ce.name)) LIKE '% CT %' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_mri_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'MRI%'
                                   OR upper(trim(ce.name)) LIKE 'CT SCAN%'
                                   OR upper(trim(ce.name)) LIKE '%CAT %'
                                   OR upper(trim(ce.name)) LIKE 'CT %'
                                   OR upper(trim(ce.name)) LIKE 'CT/%'
                                   OR upper(trim(ce.name)) LIKE ' CT %'
                                   OR upper(trim(ce.name)) LIKE '%/CT %'
                                   OR upper(trim(ce.name)) LIKE '% CT'
                                   OR upper(trim(ce.name)) LIKE '% CT %'
                                   OR upper(trim(ce.name)) LIKE 'AMB%'
                                   OR upper(trim(ce.name)) LIKE 'HCD%'
                                   OR upper(trim(ce.name)) LIKE 'HCD%'
                                   OR upper(trim(ce.name)) LIKE '%HCD'
                                   OR upper(trim(ce.name)) LIKE '%HCD %'
                                   OR upper(trim(ce.name)) LIKE 'HIGH COST DRUGS%'
                                   OR upper(trim(ce.name)) LIKE '%HIGH COST DRUGS%'
                                   OR upper(trim(ce.name)) LIKE 'HIGH COST DRUGS'
                                   OR upper(trim(ce.name)) LIKE 'IMPLANT%'
                                   OR upper(trim(ce.name)) LIKE '%IMPLANT'
                                   OR upper(trim(ce.name)) LIKE '%IMPLANT%'
                                   OR upper(trim(ce.name)) LIKE 'IMPLANTS%' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_mri_hcd_imp_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'IMPLANT%'
                                   OR upper(trim(ce.name)) LIKE '%IMPLANT'
                                   OR upper(trim(ce.name)) LIKE '%IMPLANT%'
                                   OR upper(trim(ce.name)) LIKE 'IMPLANTS%' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_imp_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE '%LAB %'
                                   OR upper(trim(ce.name)) LIKE 'LAB %'
                                   OR upper(trim(ce.name)) LIKE '%LAB'
                                   OR upper(trim(ce.name)) LIKE 'LABORATORY%'
                                   OR upper(trim(ce.name)) LIKE '%LABORATORY%'
                                   OR upper(trim(ce.name)) LIKE '%LABS%'
                                   OR upper(trim(ce.name)) LIKE '%LAB/%'
                                   OR upper(trim(ce.name)) LIKE 'LABS%'
                                   OR upper(trim(ce.name)) LIKE 'LAB %' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_lab_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'LAB %'
                                   OR upper(trim(ce.name)) LIKE '%LAB %'
                                   OR upper(trim(ce.name)) LIKE 'LAB %'
                                   OR upper(trim(ce.name)) LIKE '%LAB'
                                   OR upper(trim(ce.name)) LIKE 'LABORATORY%'
                                   OR upper(trim(ce.name)) LIKE '%LABORATORY%'
                                   OR upper(trim(ce.name)) LIKE '%LABS%'
                                   OR upper(trim(ce.name)) LIKE '%LAB/%'
                                   OR upper(trim(ce.name)) LIKE 'LABS%' THEN mapcsvc.service_charges
                          END, CAST(0 AS NUMERIC))) AS srv_lab_service_charges,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'MEDICARE - LAB%' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_mcrlab_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'TRICARE - CMAC%' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_tricmac_exp_payment,
             sum(coalesce(CASE
                              WHEN mapcsvc.ce_rule_type_id = 4
                                   AND (upper(trim(ce.name)) LIKE 'ELECTROCONVULSIVE%'
                                        OR upper(trim(ce.name)) LIKE 'ECT%'
                                        OR upper(trim(ce.name)) LIKE '%(ECT)%') THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_ect_exp_payment,
             sum(coalesce(CASE
                              WHEN upper(trim(ce.name)) LIKE 'MEDICARE - LAB%'
                                   OR upper(trim(ce.name)) LIKE 'MEDICARE - THERAPY%'
                                   OR upper(trim(ce.name)) LIKE 'MEDICARE - THERAPY REDUCTION%' THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS srv_mcrlabtherapy_exp_payment,
             sum(coalesce(mapcsvc.hac_adjmt_amt, CAST(0 AS NUMERIC))) AS hac_adjmt_amt,
             sum(coalesce(CASE
                              WHEN mapcsvc.ce_rule_type_id IN(4, 17, 40, 76, 79) THEN mapcsvc.expected_payment
                          END, CAST(0 AS NUMERIC))) AS calc_fee_schedule_adj,
             sum(coalesce(mapcsvc.gvt_device_offset_reduction, CAST(0 AS NUMERIC))) AS gvt_device_offset_reduction,
             sum(coalesce(mapcsvc.gvt_scp_payment, CAST(0 AS NUMERIC))) AS gvt_scp_payment,
             sum(coalesce(mapcsvc.val_based_adjmt_amt, CAST(0 AS NUMERIC))) AS val_based_adjmt_amt,
             sum(coalesce(mapcsvc.mdh_add_on_amt, CAST(0 AS NUMERIC))) AS mdh_add_on_amt,
             sum(coalesce(mapcsvc.readm_adjmt_amt, CAST(0 AS NUMERIC))) AS readm_adjmt_amt,
             sum(coalesce(mapcsvc.low_vol_add_on_amt, CAST(0 AS NUMERIC))) AS low_vol_add_on_amt,
             sum(CASE
                     WHEN upper(trim(ce.name)) LIKE '%PASS THROUGH%'
                          OR upper(trim(ce.name)) LIKE 'PX-T%' THEN coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))
                     ELSE CAST(0 AS NUMERIC)
                 END) AS passthrough_amount
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl_0 ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl_0.id
      AND mapcsvc.schema_id = mapcl_0.schema_id
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ce_service AS ce ON mapcsvc.ce_service_id = ce.id
      AND mapcsvc.schema_id = ce.schema_id
      GROUP BY 1,
               2) AS svc_gvt ON svc_gvt.mon_account_payer_id = clmax.mon_account_payer_id
   AND svc_gvt.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  Calc ATP

     (SELECT mapcl_0.mon_account_payer_id AS mon_account_payer_id,
             mapcl_0.schema_id AS schema_id,
             sum(coalesce(atp.expected_payment, CAST(0 AS NUMERIC))) AS calc_atp_exp_payment,
             sum(coalesce(atp.charge_amount, CAST(0 AS NUMERIC))) AS calc_atp_charge_amount
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_atp AS atp
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl_0 ON atp.mon_acct_payer_calc_summary_id = mapcl_0.id
      AND atp.schema_id = mapcl_0.schema_id
      GROUP BY 1,
               2) AS calc_atp ON calc_atp.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_atp.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  Calc APC

     (SELECT mapcl_0.mon_account_payer_id AS mon_account_payer_id,
             mapcl_0.schema_id AS schema_id,
             sum(coalesce(apc.apc_outlier_payment, CAST(0 AS NUMERIC))) AS calc_apc_outlier_payment,
             sum(coalesce(apc.expected_payment, CAST(0 AS NUMERIC))) AS calc_apc_exp_payment,
             sum(coalesce(apc.apc_deductible_amount, CAST(0 AS NUMERIC))) AS calc_apc_deductible_amount,
             sum(coalesce(apc.apc_coinsurance_amount, CAST(0 AS NUMERIC))) AS calc_apc_coinsurance_amount
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_apc AS apc
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl_0 ON apc.mon_acct_payer_calc_summary_id = mapcl_0.id
      AND apc.schema_id = mapcl_0.schema_id
      GROUP BY 1,
               2) AS calc_apc ON calc_apc.mon_account_payer_id = clmax.mon_account_payer_id
   AND calc_apc.schema_id = clmax.schema_id
   LEFT OUTER JOIN --  LOA

     (SELECT maloa.mon_account_id AS mon_account_id,
             maloa.schema_id AS schema_id,
             count(coalesce(maloa.id, CAST(0 AS BIGNUMERIC))) AS loa_count
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_loa AS maloa
      WHERE maloa.reason_code IS NOT NULL
      GROUP BY 1,
               2) AS loa ON loa.mon_account_id = ma.id
   AND loa.schema_id = ma.schema_id
   LEFT OUTER JOIN --  Remits

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
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated AS ca ON ca.ra_claim_payment_id = racp.id
      AND ca.schema_id = racp.schema_id
      WHERE racp.is_deleted <> 1
      GROUP BY 1,
               2) AS rmt ON rmt.mon_account_payer_id = mapyr.id
   AND rmt.schema_id = mapyr.schema_id
   CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%11d', CAST(mapyr.payer_rank AS INT64))), 1, 11) ]) AS iplan_insurance_order_num
   WHERE mapcl.service_date_begin < current_date('US/Central') ) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (upper(coalesce(mt.iplan_insurance_order_num, '0')) = upper(coalesce(ms.iplan_insurance_order_num, '0'))
     AND upper(coalesce(mt.iplan_insurance_order_num, '1')) = upper(coalesce(ms.iplan_insurance_order_num, '1')))
AND (coalesce(mt.eor_log_date, DATETIME '1970-01-01 00:00:00') = coalesce(ms.eor_log_date, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.eor_log_date, DATETIME '1970-01-01 00:00:01') = coalesce(ms.eor_log_date, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.eff_from_date, DATE '1970-01-01') = coalesce(ms.eff_from_date, DATE '1970-01-01')
     AND coalesce(mt.eff_from_date, DATE '1970-01-02') = coalesce(ms.eff_from_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.reimbursement_method_type_code, '0')) = upper(coalesce(ms.reimbursement_method_type_code, '0'))
     AND upper(coalesce(mt.reimbursement_method_type_code, '1')) = upper(coalesce(ms.reimbursement_method_type_code, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.eor_pat_acct_num, NUMERIC '0') = coalesce(ms.eor_pat_acct_num, NUMERIC '0')
     AND coalesce(mt.eor_pat_acct_num, NUMERIC '1') = coalesce(ms.eor_pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.eor_iplan_id, 0) = coalesce(ms.eor_iplan_id, 0)
     AND coalesce(mt.eor_iplan_id, 1) = coalesce(ms.eor_iplan_id, 1))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (upper(coalesce(mt.rate_schedule, '0')) = upper(coalesce(ms.rate_schedule, '0'))
     AND upper(coalesce(mt.rate_schedule, '1')) = upper(coalesce(ms.rate_schedule, '1')))
AND (coalesce(mt.mon_account_id, NUMERIC '0') = coalesce(ms.mon_account_id, NUMERIC '0')
     AND coalesce(mt.mon_account_id, NUMERIC '1') = coalesce(ms.mon_account_id, NUMERIC '1'))
AND (coalesce(mt.mon_payer_id, NUMERIC '0') = coalesce(ms.mon_payer_id, NUMERIC '0')
     AND coalesce(mt.mon_payer_id, NUMERIC '1') = coalesce(ms.mon_payer_id, NUMERIC '1'))
AND (coalesce(mt.schema_id, NUMERIC '0') = coalesce(ms.schema_id, NUMERIC '0')
     AND coalesce(mt.schema_id, NUMERIC '1') = coalesce(ms.schema_id, NUMERIC '1'))
AND (coalesce(mt.mon_account_payer_id, NUMERIC '0') = coalesce(ms.mon_account_payer_id, NUMERIC '0')
     AND coalesce(mt.mon_account_payer_id, NUMERIC '1') = coalesce(ms.mon_account_payer_id, NUMERIC '1'))
AND (coalesce(mt.map_misc_amt02, NUMERIC '0') = coalesce(ms.map_misc_amt02, NUMERIC '0')
     AND coalesce(mt.map_misc_amt02, NUMERIC '1') = coalesce(ms.map_misc_amt02, NUMERIC '1'))
AND (coalesce(mt.map_misc_amt01, NUMERIC '0') = coalesce(ms.map_misc_amt01, NUMERIC '0')
     AND coalesce(mt.map_misc_amt01, NUMERIC '1') = coalesce(ms.map_misc_amt01, NUMERIC '1'))
AND (coalesce(mt.map_copay, NUMERIC '0') = coalesce(ms.map_copay, NUMERIC '0')
     AND coalesce(mt.map_copay, NUMERIC '1') = coalesce(ms.map_copay, NUMERIC '1'))
AND (coalesce(mt.map_coinsurance, NUMERIC '0') = coalesce(ms.map_coinsurance, NUMERIC '0')
     AND coalesce(mt.map_coinsurance, NUMERIC '1') = coalesce(ms.map_coinsurance, NUMERIC '1'))
AND (coalesce(mt.map_deductible, NUMERIC '0') = coalesce(ms.map_deductible, NUMERIC '0')
     AND coalesce(mt.map_deductible, NUMERIC '1') = coalesce(ms.map_deductible, NUMERIC '1'))
AND (coalesce(mt.map_covered_charges, NUMERIC '0') = coalesce(ms.map_covered_charges, NUMERIC '0')
     AND coalesce(mt.map_covered_charges, NUMERIC '1') = coalesce(ms.map_covered_charges, NUMERIC '1'))
AND (coalesce(mt.map_total_payments, NUMERIC '0') = coalesce(ms.map_total_payments, NUMERIC '0')
     AND coalesce(mt.map_total_payments, NUMERIC '1') = coalesce(ms.map_total_payments, NUMERIC '1'))
AND (coalesce(mt.map_total_variance_adj, NUMERIC '0') = coalesce(ms.map_total_variance_adj, NUMERIC '0')
     AND coalesce(mt.map_total_variance_adj, NUMERIC '1') = coalesce(ms.map_total_variance_adj, NUMERIC '1'))
AND (coalesce(mt.map_total_exp_payment, NUMERIC '0') = coalesce(ms.map_total_exp_payment, NUMERIC '0')
     AND coalesce(mt.map_total_exp_payment, NUMERIC '1') = coalesce(ms.map_total_exp_payment, NUMERIC '1'))
AND (coalesce(mt.map_sequestration_amt, NUMERIC '0') = coalesce(ms.map_sequestration_amt, NUMERIC '0')
     AND coalesce(mt.map_sequestration_amt, NUMERIC '1') = coalesce(ms.map_sequestration_amt, NUMERIC '1'))
AND (coalesce(mt.mapcl_tot_exp_payment, NUMERIC '0') = coalesce(ms.mapcl_tot_exp_payment, NUMERIC '0')
     AND coalesce(mt.mapcl_tot_exp_payment, NUMERIC '1') = coalesce(ms.mapcl_tot_exp_payment, NUMERIC '1'))
AND (coalesce(mt.mapcl_base_exp_payment, NUMERIC '0') = coalesce(ms.mapcl_base_exp_payment, NUMERIC '0')
     AND coalesce(mt.mapcl_base_exp_payment, NUMERIC '1') = coalesce(ms.mapcl_base_exp_payment, NUMERIC '1'))
AND (coalesce(mt.mapcl_exclusion_exp_payment, NUMERIC '0') = coalesce(ms.mapcl_exclusion_exp_payment, NUMERIC '0')
     AND coalesce(mt.mapcl_exclusion_exp_payment, NUMERIC '1') = coalesce(ms.mapcl_exclusion_exp_payment, NUMERIC '1'))
AND (coalesce(mt.mapcl_stoploss_exp_payment, NUMERIC '0') = coalesce(ms.mapcl_stoploss_exp_payment, NUMERIC '0')
     AND coalesce(mt.mapcl_stoploss_exp_payment, NUMERIC '1') = coalesce(ms.mapcl_stoploss_exp_payment, NUMERIC '1'))
AND (coalesce(mt.ma_total_charges, NUMERIC '0') = coalesce(ms.ma_total_charges, NUMERIC '0')
     AND coalesce(mt.ma_total_charges, NUMERIC '1') = coalesce(ms.ma_total_charges, NUMERIC '1'))
AND (coalesce(mt.ma_billed_charges, NUMERIC '0') = coalesce(ms.ma_billed_charges, NUMERIC '0')
     AND coalesce(mt.ma_billed_charges, NUMERIC '1') = coalesce(ms.ma_billed_charges, NUMERIC '1'))
AND (coalesce(mt.ma_misc_amt01, NUMERIC '0') = coalesce(ms.ma_misc_amt01, NUMERIC '0')
     AND coalesce(mt.ma_misc_amt01, NUMERIC '1') = coalesce(ms.ma_misc_amt01, NUMERIC '1'))
AND (coalesce(mt.gvt_capital_drg_pmt, NUMERIC '0') = coalesce(ms.gvt_capital_drg_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_capital_drg_pmt, NUMERIC '1') = coalesce(ms.gvt_capital_drg_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_capital_outlier_pmt, NUMERIC '0') = coalesce(ms.gvt_capital_outlier_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_capital_outlier_pmt, NUMERIC '1') = coalesce(ms.gvt_capital_outlier_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_operating_fed_pmt, NUMERIC '0') = coalesce(ms.gvt_operating_fed_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_operating_fed_pmt, NUMERIC '1') = coalesce(ms.gvt_operating_fed_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_ime_operating_drg_pmt, NUMERIC '0') = coalesce(ms.gvt_ime_operating_drg_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_ime_operating_drg_pmt, NUMERIC '1') = coalesce(ms.gvt_ime_operating_drg_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_outlier_payment, NUMERIC '0') = coalesce(ms.gvt_outlier_payment, NUMERIC '0')
     AND coalesce(mt.gvt_outlier_payment, NUMERIC '1') = coalesce(ms.gvt_outlier_payment, NUMERIC '1'))
AND (coalesce(mt.gvt_dsh_operating_drg_pmt, NUMERIC '0') = coalesce(ms.gvt_dsh_operating_drg_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_dsh_operating_drg_pmt, NUMERIC '1') = coalesce(ms.gvt_dsh_operating_drg_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_capital_cost_outlier, NUMERIC '0') = coalesce(ms.gvt_capital_cost_outlier, NUMERIC '0')
     AND coalesce(mt.gvt_capital_cost_outlier, NUMERIC '1') = coalesce(ms.gvt_capital_cost_outlier, NUMERIC '1'))
AND (coalesce(mt.gvt_newtech_add_on, NUMERIC '0') = coalesce(ms.gvt_newtech_add_on, NUMERIC '0')
     AND coalesce(mt.gvt_newtech_add_on, NUMERIC '1') = coalesce(ms.gvt_newtech_add_on, NUMERIC '1'))
AND (coalesce(mt.tot_expected_payment, NUMERIC '0') = coalesce(ms.tot_expected_payment, NUMERIC '0')
     AND coalesce(mt.tot_expected_payment, NUMERIC '1') = coalesce(ms.tot_expected_payment, NUMERIC '1'))
AND (coalesce(mt.noncovered_charges, NUMERIC '0') = coalesce(ms.noncovered_charges, NUMERIC '0')
     AND coalesce(mt.noncovered_charges, NUMERIC '1') = coalesce(ms.noncovered_charges, NUMERIC '1'))
AND (coalesce(mt.gvt_drg_pmt, NUMERIC '0') = coalesce(ms.gvt_drg_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_drg_pmt, NUMERIC '1') = coalesce(ms.gvt_drg_pmt, NUMERIC '1'))
AND (coalesce(mt.gvt_cost_threshold, NUMERIC '0') = coalesce(ms.gvt_cost_threshold, NUMERIC '0')
     AND coalesce(mt.gvt_cost_threshold, NUMERIC '1') = coalesce(ms.gvt_cost_threshold, NUMERIC '1'))
AND (coalesce(mt.ip_gvt_capital_cost, NUMERIC '0') = coalesce(ms.ip_gvt_capital_cost, NUMERIC '0')
     AND coalesce(mt.ip_gvt_capital_cost, NUMERIC '1') = coalesce(ms.ip_gvt_capital_cost, NUMERIC '1'))
AND (coalesce(mt.ip_gvt_dsh_capital_drg_pmt, NUMERIC '0') = coalesce(ms.ip_gvt_dsh_capital_drg_pmt, NUMERIC '0')
     AND coalesce(mt.ip_gvt_dsh_capital_drg_pmt, NUMERIC '1') = coalesce(ms.ip_gvt_dsh_capital_drg_pmt, NUMERIC '1'))
AND (coalesce(mt.ip_gvt_adj_capital_fed_pmt, NUMERIC '0') = coalesce(ms.ip_gvt_adj_capital_fed_pmt, NUMERIC '0')
     AND coalesce(mt.ip_gvt_adj_capital_fed_pmt, NUMERIC '1') = coalesce(ms.ip_gvt_adj_capital_fed_pmt, NUMERIC '1'))
AND (coalesce(mt.dsh_uncmps_care_addon_amt, NUMERIC '0') = coalesce(ms.dsh_uncmps_care_addon_amt, NUMERIC '0')
     AND coalesce(mt.dsh_uncmps_care_addon_amt, NUMERIC '1') = coalesce(ms.dsh_uncmps_care_addon_amt, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_out_per_diem_add_on, NUMERIC '0') = coalesce(ms.ipf_pps_out_per_diem_add_on, NUMERIC '0')
     AND coalesce(mt.ipf_pps_out_per_diem_add_on, NUMERIC '1') = coalesce(ms.ipf_pps_out_per_diem_add_on, NUMERIC '1'))
AND (coalesce(mt.ipf_pps_adj_per_diem, NUMERIC '0') = coalesce(ms.ipf_pps_adj_per_diem, NUMERIC '0')
     AND coalesce(mt.ipf_pps_adj_per_diem, NUMERIC '1') = coalesce(ms.ipf_pps_adj_per_diem, NUMERIC '1'))
AND (coalesce(mt.quantity, 0) = coalesce(ms.quantity, 0)
     AND coalesce(mt.quantity, 1) = coalesce(ms.quantity, 1))
AND (coalesce(mt.comp_flg_exp_payment, NUMERIC '0') = coalesce(ms.comp_flg_exp_payment, NUMERIC '0')
     AND coalesce(mt.comp_flg_exp_payment, NUMERIC '1') = coalesce(ms.comp_flg_exp_payment, NUMERIC '1'))
AND (coalesce(mt.comp_flg_apc_outlier_amount, NUMERIC '0') = coalesce(ms.comp_flg_apc_outlier_amount, NUMERIC '0')
     AND coalesce(mt.comp_flg_apc_outlier_amount, NUMERIC '1') = coalesce(ms.comp_flg_apc_outlier_amount, NUMERIC '1'))
AND (coalesce(mt.comp_flg_apc_coinsurance_amt, NUMERIC '0') = coalesce(ms.comp_flg_apc_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.comp_flg_apc_coinsurance_amt, NUMERIC '1') = coalesce(ms.comp_flg_apc_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.comp_flg_apc_deductible_amt, NUMERIC '0') = coalesce(ms.comp_flg_apc_deductible_amt, NUMERIC '0')
     AND coalesce(mt.comp_flg_apc_deductible_amt, NUMERIC '1') = coalesce(ms.comp_flg_apc_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.srv_mcrtherapy_exp_payment, NUMERIC '0') = coalesce(ms.srv_mcrtherapy_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mcrtherapy_exp_payment, NUMERIC '1') = coalesce(ms.srv_mcrtherapy_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_hcd_exp_payment, NUMERIC '0') = coalesce(ms.srv_hcd_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_hcd_exp_payment, NUMERIC '1') = coalesce(ms.srv_hcd_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_mri_exp_payment, NUMERIC '0') = coalesce(ms.srv_mri_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mri_exp_payment, NUMERIC '1') = coalesce(ms.srv_mri_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_mri_hcd_imp_exp_payment, NUMERIC '0') = coalesce(ms.srv_mri_hcd_imp_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mri_hcd_imp_exp_payment, NUMERIC '1') = coalesce(ms.srv_mri_hcd_imp_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_imp_exp_payment, NUMERIC '0') = coalesce(ms.srv_imp_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_imp_exp_payment, NUMERIC '1') = coalesce(ms.srv_imp_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_lab_exp_payment, NUMERIC '0') = coalesce(ms.srv_lab_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_lab_exp_payment, NUMERIC '1') = coalesce(ms.srv_lab_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_lab_service_charges, NUMERIC '0') = coalesce(ms.srv_lab_service_charges, NUMERIC '0')
     AND coalesce(mt.srv_lab_service_charges, NUMERIC '1') = coalesce(ms.srv_lab_service_charges, NUMERIC '1'))
AND (coalesce(mt.srv_mcrlab_exp_payment, NUMERIC '0') = coalesce(ms.srv_mcrlab_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mcrlab_exp_payment, NUMERIC '1') = coalesce(ms.srv_mcrlab_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_tricmac_exp_payment, NUMERIC '0') = coalesce(ms.srv_tricmac_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_tricmac_exp_payment, NUMERIC '1') = coalesce(ms.srv_tricmac_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_ect_exp_payment, NUMERIC '0') = coalesce(ms.srv_ect_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_ect_exp_payment, NUMERIC '1') = coalesce(ms.srv_ect_exp_payment, NUMERIC '1'))
AND (coalesce(mt.srv_mcrlabtherapy_exp_payment, NUMERIC '0') = coalesce(ms.srv_mcrlabtherapy_exp_payment, NUMERIC '0')
     AND coalesce(mt.srv_mcrlabtherapy_exp_payment, NUMERIC '1') = coalesce(ms.srv_mcrlabtherapy_exp_payment, NUMERIC '1'))
AND (coalesce(mt.calc_atp_exp_payment, NUMERIC '0') = coalesce(ms.calc_atp_exp_payment, NUMERIC '0')
     AND coalesce(mt.calc_atp_exp_payment, NUMERIC '1') = coalesce(ms.calc_atp_exp_payment, NUMERIC '1'))
AND (coalesce(mt.calc_atp_charge_amount, NUMERIC '0') = coalesce(ms.calc_atp_charge_amount, NUMERIC '0')
     AND coalesce(mt.calc_atp_charge_amount, NUMERIC '1') = coalesce(ms.calc_atp_charge_amount, NUMERIC '1'))
AND (coalesce(mt.calc_apc_outlier_payment, NUMERIC '0') = coalesce(ms.calc_apc_outlier_payment, NUMERIC '0')
     AND coalesce(mt.calc_apc_outlier_payment, NUMERIC '1') = coalesce(ms.calc_apc_outlier_payment, NUMERIC '1'))
AND (coalesce(mt.calc_apc_exp_payment, NUMERIC '0') = coalesce(ms.calc_apc_exp_payment, NUMERIC '0')
     AND coalesce(mt.calc_apc_exp_payment, NUMERIC '1') = coalesce(ms.calc_apc_exp_payment, NUMERIC '1'))
AND (coalesce(mt.calc_apc_deductible_amount, NUMERIC '0') = coalesce(ms.calc_apc_deductible_amount, NUMERIC '0')
     AND coalesce(mt.calc_apc_deductible_amount, NUMERIC '1') = coalesce(ms.calc_apc_deductible_amount, NUMERIC '1'))
AND (coalesce(mt.calc_apc_coinsurance_amount, NUMERIC '0') = coalesce(ms.calc_apc_coinsurance_amount, NUMERIC '0')
     AND coalesce(mt.calc_apc_coinsurance_amount, NUMERIC '1') = coalesce(ms.calc_apc_coinsurance_amount, NUMERIC '1'))
AND (coalesce(mt.loa_count, 0) = coalesce(ms.loa_count, 0)
     AND coalesce(mt.loa_count, 1) = coalesce(ms.loa_count, 1))
AND (coalesce(mt.hac_adjmt_amt, NUMERIC '0') = coalesce(ms.hac_adjmt_amt, NUMERIC '0')
     AND coalesce(mt.hac_adjmt_amt, NUMERIC '1') = coalesce(ms.hac_adjmt_amt, NUMERIC '1'))
AND (coalesce(mt.ra_remit_received_flag, 0) = coalesce(ms.remit_received_flag, 0)
     AND coalesce(mt.ra_remit_received_flag, 1) = coalesce(ms.remit_received_flag, 1))
AND (coalesce(mt.ra_deductible, NUMERIC '0') = coalesce(ms.ra_deductible, NUMERIC '0')
     AND coalesce(mt.ra_deductible, NUMERIC '1') = coalesce(ms.ra_deductible, NUMERIC '1'))
AND (coalesce(mt.ra_coinsurance, NUMERIC '0') = coalesce(ms.ra_coinsurance, NUMERIC '0')
     AND coalesce(mt.ra_coinsurance, NUMERIC '1') = coalesce(ms.ra_coinsurance, NUMERIC '1'))
AND (coalesce(mt.ra_copay, NUMERIC '0') = coalesce(ms.ra_copay, NUMERIC '0')
     AND coalesce(mt.ra_copay, NUMERIC '1') = coalesce(ms.ra_copay, NUMERIC '1'))
AND (coalesce(mt.ra_pat_resp_non_covered_amt, NUMERIC '0') = coalesce(ms.ra_pt_resp_non_covered_amt, NUMERIC '0')
     AND coalesce(mt.ra_pat_resp_non_covered_amt, NUMERIC '1') = coalesce(ms.ra_pt_resp_non_covered_amt, NUMERIC '1'))
AND (coalesce(mt.calc_fee_schedule_adj, NUMERIC '0') = coalesce(ms.calc_fee_schedule_adj, NUMERIC '0')
     AND coalesce(mt.calc_fee_schedule_adj, NUMERIC '1') = coalesce(ms.calc_fee_schedule_adj, NUMERIC '1'))
AND (coalesce(mt.acct_subterm_amt, NUMERIC '0') = coalesce(ms.acct_subterm_amt, NUMERIC '0')
     AND coalesce(mt.acct_subterm_amt, NUMERIC '1') = coalesce(ms.acct_subterm_amt, NUMERIC '1'))
AND (coalesce(mt.gvt_device_offset_reduction, NUMERIC '0') = coalesce(ms.gvt_device_offset_reduction, NUMERIC '0')
     AND coalesce(mt.gvt_device_offset_reduction, NUMERIC '1') = coalesce(ms.gvt_device_offset_reduction, NUMERIC '1'))
AND (coalesce(mt.gvt_scp_payment, NUMERIC '0') = coalesce(ms.gvt_scp_payment, NUMERIC '0')
     AND coalesce(mt.gvt_scp_payment, NUMERIC '1') = coalesce(ms.gvt_scp_payment, NUMERIC '1'))
AND (coalesce(mt.val_based_adjmt_amt, NUMERIC '0') = coalesce(ms.val_based_adjmt_amt, NUMERIC '0')
     AND coalesce(mt.val_based_adjmt_amt, NUMERIC '1') = coalesce(ms.val_based_adjmt_amt, NUMERIC '1'))
AND (coalesce(mt.mdh_add_on_amt, NUMERIC '0') = coalesce(ms.mdh_add_on_amt, NUMERIC '0')
     AND coalesce(mt.mdh_add_on_amt, NUMERIC '1') = coalesce(ms.mdh_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.readm_adjmt_amt, NUMERIC '0') = coalesce(ms.readm_adjmt_amt, NUMERIC '0')
     AND coalesce(mt.readm_adjmt_amt, NUMERIC '1') = coalesce(ms.readm_adjmt_amt, NUMERIC '1'))
AND (coalesce(mt.low_vol_add_on_amt, NUMERIC '0') = coalesce(ms.low_vol_add_on_amt, NUMERIC '0')
     AND coalesce(mt.low_vol_add_on_amt, NUMERIC '1') = coalesce(ms.low_vol_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.cob_amount, NUMERIC '0') = coalesce(ms.cob_amount, NUMERIC '0')
     AND coalesce(mt.cob_amount, NUMERIC '1') = coalesce(ms.cob_amount, NUMERIC '1'))
AND (coalesce(mt.passthrough_amount, NUMERIC '0') = coalesce(ms.passthrough_amount, NUMERIC '0')
     AND coalesce(mt.passthrough_amount, NUMERIC '1') = coalesce(ms.passthrough_amount, NUMERIC '1'))
AND (coalesce(mt.gvt_operating_outlier_pmt, NUMERIC '0') = coalesce(ms.gvt_operating_outlier_pmt, NUMERIC '0')
     AND coalesce(mt.gvt_operating_outlier_pmt, NUMERIC '1') = coalesce(ms.gvt_operating_outlier_pmt, NUMERIC '1')) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        reimbursement_method_type_code,
        unit_num,
        eor_pat_acct_num,
        eor_iplan_id,
        dw_last_update_date_time,
        source_system_code,
        rate_schedule,
        mon_account_id,
        mon_payer_id,
        schema_id,
        mon_account_payer_id,
        map_misc_amt02,
        map_misc_amt01,
        map_copay,
        map_coinsurance,
        map_deductible,
        map_covered_charges,
        map_total_payments,
        map_total_variance_adj,
        map_total_exp_payment,
        map_sequestration_amt,
        mapcl_tot_exp_payment,
        mapcl_base_exp_payment,
        mapcl_exclusion_exp_payment,
        mapcl_stoploss_exp_payment,
        ma_total_charges,
        ma_billed_charges,
        ma_misc_amt01,
        gvt_capital_drg_pmt,
        gvt_capital_outlier_pmt,
        gvt_operating_fed_pmt,
        gvt_ime_operating_drg_pmt,
        gvt_outlier_payment,
        gvt_dsh_operating_drg_pmt,
        gvt_capital_cost_outlier,
        gvt_newtech_add_on,
        tot_expected_payment,
        noncovered_charges,
        gvt_drg_pmt,
        gvt_cost_threshold,
        ip_gvt_capital_cost,
        ip_gvt_dsh_capital_drg_pmt,
        ip_gvt_adj_capital_fed_pmt,
        dsh_uncmps_care_addon_amt,
        ipf_pps_out_per_diem_add_on,
        ipf_pps_adj_per_diem,
        quantity,
        comp_flg_exp_payment,
        comp_flg_apc_outlier_amount,
        comp_flg_apc_coinsurance_amt,
        comp_flg_apc_deductible_amt,
        srv_mcrtherapy_exp_payment,
        srv_hcd_exp_payment,
        srv_mri_exp_payment,
        srv_mri_hcd_imp_exp_payment,
        srv_imp_exp_payment,
        srv_lab_exp_payment,
        srv_lab_service_charges,
        srv_mcrlab_exp_payment,
        srv_tricmac_exp_payment,
        srv_ect_exp_payment,
        srv_mcrlabtherapy_exp_payment,
        calc_atp_exp_payment,
        calc_atp_charge_amount,
        calc_apc_outlier_payment,
        calc_apc_exp_payment,
        calc_apc_deductible_amount,
        calc_apc_coinsurance_amount,
        loa_count,
        hac_adjmt_amt,
        ra_remit_received_flag,
        ra_deductible,
        ra_coinsurance,
        ra_copay,
        ra_pat_resp_non_covered_amt,
        calc_fee_schedule_adj,
        acct_subterm_amt,
        gvt_device_offset_reduction,
        gvt_scp_payment,
        val_based_adjmt_amt,
        mdh_add_on_amt,
        readm_adjmt_amt,
        low_vol_add_on_amt,
        cob_amount,
        passthrough_amount,
        gvt_operating_outlier_pmt)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.reimbursement_method_type_code, ms.unit_num, ms.eor_pat_acct_num, ms.eor_iplan_id, ms.dw_last_update_date_time, ms.source_system_code, ms.rate_schedule, ms.mon_account_id, ms.mon_payer_id, ms.schema_id, ms.mon_account_payer_id, ms.map_misc_amt02, ms.map_misc_amt01, ms.map_copay, ms.map_coinsurance, ms.map_deductible, ms.map_covered_charges, ms.map_total_payments, ms.map_total_variance_adj, ms.map_total_exp_payment, ms.map_sequestration_amt, ms.mapcl_tot_exp_payment, ms.mapcl_base_exp_payment, ms.mapcl_exclusion_exp_payment, ms.mapcl_stoploss_exp_payment, ms.ma_total_charges, ms.ma_billed_charges, ms.ma_misc_amt01, ms.gvt_capital_drg_pmt, ms.gvt_capital_outlier_pmt, ms.gvt_operating_fed_pmt, ms.gvt_ime_operating_drg_pmt, ms.gvt_outlier_payment, ms.gvt_dsh_operating_drg_pmt, ms.gvt_capital_cost_outlier, ms.gvt_newtech_add_on, ms.tot_expected_payment, ms.noncovered_charges, ms.gvt_drg_pmt, ms.gvt_cost_threshold, ms.ip_gvt_capital_cost, ms.ip_gvt_dsh_capital_drg_pmt, ms.ip_gvt_adj_capital_fed_pmt, ms.dsh_uncmps_care_addon_amt, ms.ipf_pps_out_per_diem_add_on, ms.ipf_pps_adj_per_diem, ms.quantity, ms.comp_flg_exp_payment, ms.comp_flg_apc_outlier_amount, ms.comp_flg_apc_coinsurance_amt, ms.comp_flg_apc_deductible_amt, ms.srv_mcrtherapy_exp_payment, ms.srv_hcd_exp_payment, ms.srv_mri_exp_payment, ms.srv_mri_hcd_imp_exp_payment, ms.srv_imp_exp_payment, ms.srv_lab_exp_payment, ms.srv_lab_service_charges, ms.srv_mcrlab_exp_payment, ms.srv_tricmac_exp_payment, ms.srv_ect_exp_payment, ms.srv_mcrlabtherapy_exp_payment, ms.calc_atp_exp_payment, ms.calc_atp_charge_amount, ms.calc_apc_outlier_payment, ms.calc_apc_exp_payment, ms.calc_apc_deductible_amount, ms.calc_apc_coinsurance_amount, ms.loa_count, ms.hac_adjmt_amt, ms.remit_received_flag, ms.ra_deductible, ms.ra_coinsurance, ms.ra_copay, ms.ra_pt_resp_non_covered_amt, ms.calc_fee_schedule_adj, ms.acct_subterm_amt, ms.gvt_device_offset_reduction, ms.gvt_scp_payment, ms.val_based_adjmt_amt, ms.mdh_add_on_amt, ms.readm_adjmt_amt, ms.low_vol_add_on_amt, ms.cob_amount, ms.passthrough_amount, ms.gvt_operating_outlier_pmt);


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
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.eor_amount_stg_calc_service');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_STAGING','Eor_Amount_Stg_Calc_Service');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;