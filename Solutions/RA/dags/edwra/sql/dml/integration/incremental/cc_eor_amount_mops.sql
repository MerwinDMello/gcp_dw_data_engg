DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_amount_mops.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Holly Ray
       Date: 11/22/2011
       Name: CC_EOR_Amount_MOPS.sql
       Mod1: Changed DB logon path for DMEXpress conversion on 2/10/2012 SW.
       MOD2: ADDED composite flag filter for cat135 08/30
       Mod3: Re-wrote query to use staging tables.01/30/2014. AP.
       Mod4: Added new category code 142. 03/24/2014. AP.
	Mod5:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod6: Changed logic for category codes 30 and 86 , removed category code 142 on 5/18/2019 AM
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA147_MOPS;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nohashjoin on for session;
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
          15 AS amount_category_code,
          s.unit_num,
          s.eor_pat_acct_num,
          s.eor_iplan_id,
          s.map_misc_amt01 AS eor_reimbursement_amt,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
          'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND s.map_misc_amt01 <> 0
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
                    30 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(CASE
                                           WHEN s.cob_amount = 0 THEN s.comp_flg_exp_payment + s.calc_apc_exp_payment - (s.comp_flg_apc_outlier_amount + s.calc_apc_outlier_payment) + s.passthrough_amount + s.acct_subterm_amt + s.map_total_exp_payment - (s.mapcl_base_exp_payment + s.mapcl_exclusion_exp_payment + s.map_sequestration_amt)
                                           ELSE s.comp_flg_exp_payment + s.calc_apc_exp_payment - (s.comp_flg_apc_outlier_amount + s.calc_apc_outlier_payment) + s.passthrough_amount + s.acct_subterm_amt + s.cob_amount
                                       END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    42 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.map_misc_amt02 AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND s.map_misc_amt02 <> 0
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
                    50 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_mcrlab_exp_payment + s.calc_atp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    71 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.comp_flg_apc_outlier_amount + s.calc_apc_outlier_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    81 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.srv_mcrtherapy_exp_payment AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND s.srv_mcrtherapy_exp_payment <> 0
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
                    82 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.comp_flg_apc_outlier_amount + s.calc_apc_outlier_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    84 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_mcrlab_exp_payment + s.calc_atp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    85 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.srv_mcrtherapy_exp_payment AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND s.srv_mcrtherapy_exp_payment <> 0
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
                    86 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.tot_expected_payment - s.comp_flg_exp_payment - s.srv_mcrlabtherapy_exp_payment - s.calc_apc_exp_payment - s.calc_atp_exp_payment - s.passthrough_amount, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND eor_reimbursement_amt <> 0
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
                    93 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    s.srv_mcrtherapy_exp_payment AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   WHERE upper(trim(s.rate_schedule)) = 'MOPS'
     AND s.srv_mcrtherapy_exp_payment <> 0 ) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_code))
AND upper(trim(x.coid)) = upper(trim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.iplan_insurance_order_num) AS FLOAT64)
AND x.eor_log_date = DATE(z.eor_log_date)
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