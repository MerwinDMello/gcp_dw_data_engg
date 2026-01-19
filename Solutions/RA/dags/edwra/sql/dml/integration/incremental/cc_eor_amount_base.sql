DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_amount_base.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*********************************************************************************************************************************
  Developer: Holly Ray
       Date: 11/22/2011
       Name: CC_EOR_Amount_Base.sql
       Mod1: Changed DB logon path for DMEXpress conversion on 2/10/2012 SW.
       MOD2: ADDED composite flag filter for cat135 08/30
       Mod3: Re-wrote query to use staging tables.01/30/2014. AP.
       Mod4: Added new category code 141. 02/18/2014. AP.
       Mod5: Modified EOR Amount for category code 100 to substract Patient Responsibilit Amt. 02/19/2014. AP.
       Mod6: Added new Category Code 45 for HAC_ADJMT_AMT. 10/01/2014. AP.
       Mod7: Modified Category Code 45 to show as Positive Amt. 10/07/2014. AP.
       Mod8: Modified Category Codes 27,33,41,43,123 to include the remit logic. 10/10/2014. AP.
             Added New Category Code 145. (CR112) 10/10/2014. AP.
       Mod9: Modified Category Code 141, sequestration amount stored as positive now. (CR136) 10/10/2014. AP.
      Mod10: Adding Category Code 44. Moving from MCARE script to here. It should match to 123 Category Code. 10/21/2014. AP.
	  Mod11: PBI16380 logic for category 14 and category 114 are not calculating correctly.
	         Category 14  changed to s.Ma_Billed_Charges - s.Map_Covered_Charges.
	 	     Category 114 changed to s.Ma_Billed_Charges. on 5/15/2018 SW.
	Mod12:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
*********************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA147_BASE;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_amount AS x USING
  (SELECT s.company_code,
          s.coid,
          s.patient_dw_id,
          s.payor_dw_id,
          s.iplan_insurance_order_num,
          s.eor_log_date,
          s.log_id,
          s.log_sequence_num,
          s.eff_from_date,
          s.reimbursement_method_type_code,
          9 AS amount_category_code,
          s.unit_num,
          s.eor_pat_acct_num,
          s.eor_iplan_id,
          s.ip_gvt_capital_cost AS eor_reimbursement_amt,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.ip_gvt_capital_cost <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    10 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.ip_gvt_dsh_capital_drg_pmt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.ip_gvt_dsh_capital_drg_pmt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    11 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.ip_gvt_adj_capital_fed_pmt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.ip_gvt_adj_capital_fed_pmt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    14 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.ma_billed_charges - s.map_covered_charges, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    18 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.ma_total_charges - s.ma_billed_charges, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    27 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_coinsurance
                                ELSE s.map_coinsurance
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    32 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.map_misc_amt02 AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.map_misc_amt02 <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    33 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_deductible
                                ELSE s.map_deductible
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    39 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.gvt_drg_pmt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.gvt_drg_pmt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    41 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_deductible
                                ELSE s.map_deductible
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    43 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_coinsurance
                                ELSE s.map_coinsurance
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    44 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_copay
                                ELSE s.map_copay
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    45 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ s.hac_adjmt_amt * -1 ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    46 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.map_misc_amt01 AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.map_misc_amt01 <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    62 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.ma_billed_charges - s.map_covered_charges, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    63 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(s.loa_count AS NUMERIC) ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    70 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.gvt_cost_threshold AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.gvt_cost_threshold <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    114 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.ma_billed_charges AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.ma_billed_charges <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    117 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.map_total_payments AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.map_total_payments <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    123 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_copay
                                ELSE s.map_copay
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    124 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.map_total_variance_adj AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.map_total_variance_adj <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    134 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.ma_misc_amt01 AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE s.ma_misc_amt01 <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    135 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.comp_flg_exp_payment - s.comp_flg_apc_deductible_amt - s.comp_flg_apc_coinsurance_amt + s.calc_apc_exp_payment - s.calc_apc_deductible_amount - s.calc_apc_coinsurance_amount, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
     AND upper(trim(s.rate_schedule)) <> 'TCROP'
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    136 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(o.calc_fs_exp_payment + o.calc_coinfs_exp_payment - o.calc_coinfs_coins_amount + s.calc_atp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    79 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS pripyr ON s.mon_account_id = pripyr.mon_account_id
   AND s.schema_id = pripyr.schema_id
   AND upper(trim(s.iplan_insurance_order_num)) <> '1'
   AND pripyr.payer_rank = 1
   CROSS JOIN UNNEST(ARRAY[ ROUND(coalesce(CASE
                                               WHEN upper(trim(s.iplan_insurance_order_num)) IN('2', '3') THEN pripyr.total_payments
                                           END, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    133 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS pripyr ON s.mon_account_id = pripyr.mon_account_id
   AND s.schema_id = pripyr.schema_id
   AND upper(trim(s.iplan_insurance_order_num)) <> '1'
   AND pripyr.payer_rank = 1
   CROSS JOIN UNNEST(ARRAY[ ROUND(coalesce(CASE
                                               WHEN upper(trim(s.iplan_insurance_order_num)) IN('2', '3') THEN pripyr.total_payments
                                           END, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    100 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS pripyr ON s.mon_account_id = pripyr.mon_account_id
   AND s.schema_id = pripyr.schema_id
   AND upper(trim(s.iplan_insurance_order_num)) <> '1'
   AND pripyr.payer_rank = 1
   CROSS JOIN UNNEST(ARRAY[ ROUND(coalesce(CASE
                                               WHEN upper(trim(s.iplan_insurance_order_num)) IN('2', '3') THEN coalesce(pripyr.total_expected_payment, CAST(0 AS NUMERIC)) - coalesce(pripyr.total_pt_responsibility, CAST(0 AS NUMERIC))
                                           END, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    141 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.map_sequestration_amt * -1, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    140 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN
     (SELECT max(coalesce(app.appeal_no, NUMERIC '0')) AS max_appeal,
             app.mon_account_id,
             app.mon_payer_id,
             app.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS app
      GROUP BY 2,
               3,
               4) AS max_app ON max_app.mon_account_id = s.mon_account_id
   AND max_app.mon_payer_id = s.mon_payer_id
   AND max_app.schema_id = s.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS app_id ON app_id.mon_account_id = max_app.mon_account_id
   AND app_id.mon_payer_id = max_app.mon_payer_id
   AND app_id.appeal_no = max_app.max_appeal
   AND app_id.schema_id = max_app.schema_id
   INNER JOIN
     (SELECT max(coalesce(seq.sequence_no, NUMERIC '0')) AS max_sequence,
             seq.mon_appeal_id,
             seq.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS seq
      GROUP BY 2,
               3) AS max_seq ON max_seq.mon_appeal_id = app_id.id
   AND max_seq.schema_id = app_id.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS seq_amt ON seq_amt.mon_appeal_id = max_seq.mon_appeal_id
   AND seq_amt.schema_id = max_seq.schema_id
   AND seq_amt.sequence_no = max_seq.max_sequence
   CROSS JOIN UNNEST(ARRAY[ ROUND(coalesce(seq_amt.appeal_balance_amt, CAST(0 AS NUMERIC)), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0
   UNION ALL SELECT s.company_code,
                    s.coid,
                    s.patient_dw_id,
                    s.payor_dw_id,
                    s.iplan_insurance_order_num,
                    s.eor_log_date,
                    s.log_id,
                    s.log_sequence_num,
                    s.eff_from_date,
                    s.reimbursement_method_type_code,
                    145 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN s.ra_remit_received_flag = 1 THEN s.ra_pat_resp_non_covered_amt
                                ELSE CAST(0 AS NUMERIC)
                            END ]) AS eor_reimbursement_amt
   WHERE eor_reimbursement_amt <> 0 ) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_code))
AND upper(trim(x.coid)) = upper(trim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.iplan_insurance_order_num) AS FLOAT64)
AND DATE(x.eor_log_date) = DATE(z.eor_log_date)
AND upper(trim(x.log_id)) = upper(trim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND x.eff_from_date = z.eff_from_date
AND x.amount_category_code = z.amount_category_code WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.eor_pat_acct_num,
    iplan_id = z.eor_iplan_id,
    eor_reimbursement_amt = z.eor_reimbursement_amt,
    reimbursement_method_type_code = substr(z.reimbursement_method_type_code, 1, 1),
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
        reimbursement_method_type_code,
        amount_category_code,
        unit_num,
        pat_acct_num,
        iplan_id,
        eor_reimbursement_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.iplan_insurance_order_num) AS INT64), DATE(z.eor_log_date), substr(z.log_id, 1, 4), z.log_sequence_num, z.eff_from_date, substr(z.reimbursement_method_type_code, 1, 1), z.amount_category_code, z.unit_num, z.eor_pat_acct_num, z.eor_iplan_id, z.eor_reimbursement_amt, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


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
             eff_from_date,
             reimbursement_method_type_code,
             amount_category_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_amount
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date,
               reimbursement_method_type_code,
               amount_category_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_amount');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_EOR_Amount');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;