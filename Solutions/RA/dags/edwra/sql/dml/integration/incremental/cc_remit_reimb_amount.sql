DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_remit_reimb_amount.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Sean Wilson
       Date: 11/17/2011
       Name: CC_Remit_Reimb_Amount.sql
       Mod1: Initial creation of BTEQ script on 11/17/2011.
       Mod2: Fixed SQL for QA on 11/22/2011 SW.
       Mod3: Added new category codes to extract on 12/1/2011 SW.
       Mod4: Fixed 139 category code logic on 12/04/2011 FY.
       Mod5: Removed RA_Group_Header and Mon_Account joins for 9.6 on
             12/20/2011 SW.
       Mod6: WHERE RACP.Service_Start_Date  > DATE '2011-09-01'
             07/31/2012 FY.
       Mod7: Enhanced category code 71 and 135 based on 2013 enhancement list
             on 1/22/2013 SW.
       Mod8: Fix for defect found in QA for category code 135 on 3/8/2013 SW.
       Mod9: Fix for defect found in QA for category code 71 on 3/11/2013 SW.
       Mod10:Fix for defect found in QA for category code 71 on 3/14/2013 SW.
       Mod11:Re-wrote query to use staging table (CC_Remit_Amt_Stg). 3/17/2014 AP.
       Mod12:Added diagnostics to speed up query on 2/9/2015 SW.
       Mod13:Changed delete to only consider active coids on 1/30/2018 SW.
       Mod14:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
       Mod15:Added Audit Merge to remove expected task 07/08/2020 PT.
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA233;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nohashjoin on for session;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_reimb_amount_stg AS mt USING
  (SELECT DISTINCT s.company_code AS company_code,
                   substr(s.coid, 1, 5) AS coid,
                   s.patient_dw_id,
                   s.payor_dw_id,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                   s.ra_log_date,
                   substr(s.log_id, 1, 4) AS log_id,
                   row_number() OVER (PARTITION BY upper(s.company_code),
                                                   upper(s.coid),
                                                   s.patient_dw_id,
                                                   s.payor_dw_id,
                                                   upper(s.remittance_advice_num),
                                                   s.ra_log_date,
                                                   upper(s.log_id)
                                      ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                     CAST(8 AS NUMERIC) AS amount_category_code,
                                     substr(s.unit_num, 1, 5) AS unit_num,
                                     s.ra_pat_acct_num,
                                     s.iplan_insurance_order_num,
                                     s.ra_iplan_id,
                                     reimbursement_amt AS reimbursement_amt,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                     'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.claim_ppscapital_amount, CAST(0 AS BIGNUMERIC)) + coalesce(s.claim_pps_capital_outlier_amnt, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE upper(s.rate_schedule_name) LIKE 'GOV MIPS IP%'
     AND ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(27 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_510, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(30 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.claim_drg_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(32 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrvadj_pr_code, CAST(0 AS BIGNUMERIC)) + coalesce(s.raclaimadj_pr_code, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(33 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_550, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(46 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrv_charge_amt_cat46, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE upper(rtrim(s.financial_class)) IN('1',
                                            '6')
     AND ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(49 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrv_charge_amt_cat49, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(50 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrv_prov_pymnt_amt_cat50, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(71 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(CASE upper(rtrim(s.ip_op_ind))
                                           WHEN 'I' THEN coalesce(s.pps_operat_outlier_amount, CAST(0 AS BIGNUMERIC)) + coalesce(s.claim_pps_capital_outlier_amnt, CAST(0 AS BIGNUMERIC))
                                           WHEN 'O' THEN coalesce(s.raclaimsup_amt_zm, CAST(0 AS BIGNUMERIC))
                                       END, 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(79 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrvadj_oa_code, CAST(0 AS BIGNUMERIC)) + coalesce(s.raclaimadj_oa_code, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(88 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrv_prov_pymnt_amt_cat88, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(93 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrv_prov_pymnt_amt_cat93, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(119 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_501_599, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(123 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_520, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(133 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.rasrvadj_oa_code, CAST(0 AS BIGNUMERIC)) + coalesce(s.raclaimadj_oa_code, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(135 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.raclaimsup_amt_zm, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE upper(rtrim(s.ip_op_ind)) = 'O'
     AND ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(136 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.claim_hcpcs_payable_amount, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(137 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_301_399, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(138 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.ra835_cat_110, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0
   UNION DISTINCT SELECT DISTINCT s.company_code AS company_code,
                                  substr(s.coid, 1, 5) AS coid,
                                  s.patient_dw_id,
                                  s.payor_dw_id,
                                  CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(s.remittance_advice_num) AS INT64) AS remittance_advice_num,
                                  s.ra_log_date,
                                  substr(s.log_id, 1, 4) AS log_id,
                                  row_number() OVER (PARTITION BY upper(s.company_code),
                                                                  upper(s.coid),
                                                                  s.patient_dw_id,
                                                                  s.payor_dw_id,
                                                                  upper(s.remittance_advice_num),
                                                                  s.ra_log_date,
                                                                  upper(s.log_id)
                                                     ORDER BY s.ra_claim_payment_id) AS log_sequence_num,
                                                    CAST(139 AS NUMERIC) AS amount_category_code,
                                                    substr(s.unit_num, 1, 5) AS unit_num,
                                                    s.ra_pat_acct_num,
                                                    s.iplan_insurance_order_num,
                                                    s.ra_iplan_id,
                                                    reimbursement_amt AS reimbursement_amt,
                                                    datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                                    'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS s
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(s.claim_payment_amount, CAST(0 AS BIGNUMERIC)) + coalesce(s.ra835_cat_501_599, CAST(0 AS BIGNUMERIC)) + coalesce(s.raclaimsup_amt_i_t, CAST(0 AS BIGNUMERIC)), 3, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS reimbursement_amt
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0 ) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.remittance_advice_num, 0) = coalesce(ms.remittance_advice_num, 0)
     AND coalesce(mt.remittance_advice_num, 1) = coalesce(ms.remittance_advice_num, 1))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.amount_category_code, NUMERIC '0') = coalesce(ms.amount_category_code, NUMERIC '0')
     AND coalesce(mt.amount_category_code, NUMERIC '1') = coalesce(ms.amount_category_code, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.ra_pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.ra_pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.ra_iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.ra_iplan_id, 1))
AND (coalesce(mt.reimbursement_amt, NUMERIC '0') = coalesce(ms.reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.reimbursement_amt, NUMERIC '1') = coalesce(ms.reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        log_id,
        log_sequence_num,
        amount_category_code,
        unit_num,
        pat_acct_num,
        iplan_insurance_order_num,
        iplan_id,
        reimbursement_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.remittance_advice_num, ms.ra_log_date, ms.log_id, ms.log_sequence_num, ms.amount_category_code, ms.unit_num, ms.ra_pat_acct_num, ms.iplan_insurance_order_num, ms.ra_iplan_id, ms.reimbursement_amt, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             remittance_advice_num,
             ra_log_date,
             log_id,
             log_sequence_num,
             amount_category_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_reimb_amount_stg
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               remittance_advice_num,
               ra_log_date,
               log_id,
               log_sequence_num,
               amount_category_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_reimb_amount_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Remit_Reimb_Amount_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS x USING
  (SELECT cc_remit_reimb_amount_stg.company_code,
          cc_remit_reimb_amount_stg.coid,
          cc_remit_reimb_amount_stg.patient_dw_id,
          cc_remit_reimb_amount_stg.payor_dw_id,
          cc_remit_reimb_amount_stg.remittance_advice_num,
          cc_remit_reimb_amount_stg.ra_log_date,
          cc_remit_reimb_amount_stg.log_id,
          cc_remit_reimb_amount_stg.log_sequence_num,
          cc_remit_reimb_amount_stg.amount_category_code,
          cc_remit_reimb_amount_stg.unit_num,
          cc_remit_reimb_amount_stg.pat_acct_num,
          cc_remit_reimb_amount_stg.iplan_insurance_order_num,
          cc_remit_reimb_amount_stg.iplan_id,
          cc_remit_reimb_amount_stg.reimbursement_amt,
          cc_remit_reimb_amount_stg.dw_last_update_date_time,
          cc_remit_reimb_amount_stg.source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_reimb_amount_stg) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.remittance_advice_num = z.remittance_advice_num
AND x.ra_log_date = z.ra_log_date
AND upper(rtrim(x.log_id)) = upper(rtrim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND x.amount_category_code = z.amount_category_code WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_insurance_order_num = z.iplan_insurance_order_num,
    iplan_id = z.iplan_id,
    reimbursement_amt = z.reimbursement_amt,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        log_id,
        log_sequence_num,
        amount_category_code,
        unit_num,
        pat_acct_num,
        iplan_insurance_order_num,
        iplan_id,
        reimbursement_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.remittance_advice_num, z.ra_log_date, z.log_id, z.log_sequence_num, z.amount_category_code, z.unit_num, z.pat_acct_num, z.iplan_insurance_order_num, z.iplan_id, z.reimbursement_amt, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             remittance_advice_num,
             ra_log_date,
             log_id,
             log_sequence_num,
             amount_category_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               remittance_advice_num,
               ra_log_date,
               log_id,
               log_sequence_num,
               amount_category_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS a
WHERE upper(a.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND (upper(a.company_code),
       upper(a.coid),
       a.patient_dw_id,
       a.payor_dw_id,
       a.remittance_advice_num,
       a.ra_log_date,
       upper(a.log_id),
       a.log_sequence_num,
       a.amount_category_code) IN
    (SELECT AS STRUCT upper(max(b.company_code)) AS company_code,
                      upper(max(b.coid)) AS coid,
                      b.patient_dw_id,
                      b.payor_dw_id,
                      b.remittance_advice_num,
                      b.ra_log_date,
                      upper(max(b.log_id)) AS log_id,
                      b.log_sequence_num,
                      b.amount_category_code
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS b
     GROUP BY upper(b.company_code),
              upper(b.coid),
              3,
              4,
              5,
              6,
              upper(b.log_id),
              8,
              9
     HAVING count(*) > 1)
  AND upper(CAST(a.dw_last_update_date_time AS STRING)) NOT IN
    (SELECT upper(CAST(max(c.dw_last_update_date_time) AS STRING))
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS c
     WHERE upper(rtrim(a.company_code)) = upper(rtrim(c.company_code))
       AND upper(rtrim(a.coid)) = upper(rtrim(c.coid))
       AND a.patient_dw_id = c.patient_dw_id
       AND a.payor_dw_id = c.payor_dw_id
       AND a.remittance_advice_num = c.remittance_advice_num
       AND a.ra_log_date = c.ra_log_date
       AND upper(rtrim(a.log_id)) = upper(rtrim(c.log_id))
       AND a.log_sequence_num = c.log_sequence_num
       AND a.amount_category_code = c.amount_category_code );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Remit_Reimb_Amount');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_reimb_amount_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;