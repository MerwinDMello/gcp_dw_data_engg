DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eor_amount_mcare.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Holly Ray
       Date: 11/22/2011
       Name: CC_EOR_Amount_MCARE.sql
       Mod1: Changed DB logon path for DMEXpress conversion on 2/10/2012 SW.
       MOD2: ADDED composite flag filter for cat135 08/30
       Mod3: Re-wrote query to use staging tables.01/30/2014. AP.
       Mod4: Added new category code 142. 02/18/2014. AP.
       Mod5: Modified category code 44 to include remit logic. 10/10/2014. AP.
       Mod6: Removed Category Code 44. Moving this category to Base script. It should match to 123 Category Code. 10/21/2014. AP.
	Mod7:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod8: Removed category code 142. Added logic for category code 15. on 1/25/2019 PT
	Mod9: added condition for category code =86 by adding Eor_Reimbursement_Amt <> 0 filter.Modified logic to populate other category codes. PBI19954 on 1/25/2019 PT
	Mod10: Added 142 category code back as per requirement change.
	Mod11: Added Hac_Adjmt_Amt for amount category 30.
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA147_MCARE;');
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
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    16 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_lab_service_charges + s.calc_atp_charge_amount + o.srv_lab_service_charges, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    30 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.mapcl_base_exp_payment + s.mapcl_exclusion_exp_payment + coalesce(s.hac_adjmt_amt * -1, CAST(0 AS NUMERIC)) - s.srv_mri_hcd_imp_exp_payment - o.srv_mri_ct_amb_exp_payment - o.srv_hcd_exp_payment - o.srv_imp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    50 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_lab_exp_payment + s.calc_atp_exp_payment + o.srv_lab_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    83 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_mri_exp_payment + o.srv_mri_ct_amb_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_hcd_exp_payment + o.srv_hcd_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    86 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.map_total_exp_payment - (s.mapcl_base_exp_payment + s.mapcl_exclusion_exp_payment + s.map_sequestration_amt), 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    99 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_other_service AS o ON o.mon_account_payer_id = s.mon_account_payer_id
   AND o.schema_id = s.schema_id
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.srv_imp_exp_payment + o.srv_imp_exp_payment, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
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
                    142 AS amount_category_code,
                    s.unit_num,
                    s.eor_pat_acct_num,
                    s.eor_iplan_id,
                    eor_reimbursement_amt AS eor_reimbursement_amt,
                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.eor_amount_stg_calc_service AS s
   CROSS JOIN UNNEST(ARRAY[ ROUND(s.dsh_uncmps_care_addon_amt, 3, 'ROUND_HALF_EVEN') ]) AS eor_reimbursement_amt
   WHERE upper(trim(s.rate_schedule)) = 'MCARE'
     AND eor_reimbursement_amt <> 0 ) AS z ON upper(trim(x.company_code)) = upper(trim(z.company_code))
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