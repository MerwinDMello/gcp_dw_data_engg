##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, audit_job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = 'ra'; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','cc_remit_amt_stg' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET audit_job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list = -- This needs to be added
(
SELECT [format('%20d', a.row_count)]
FROM
  (
select count(*) as row_count from
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
   WHERE ROUND(reimbursement_amt, 2, 'ROUND_HALF_EVEN') <> 0 )
     
     ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount
        WHERE cc_remit_reimb_amount.dw_last_update_date_time >=
   (SELECT coalesce(max(audit_control.load_end_time), date_add(timestamp_trunc(current_datetime('US/Central'), SECOND), INTERVAL -1 DAY))
	FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
	WHERE upper(audit_control.job_name) = upper(audit_job_name)
	  AND audit_control.load_end_time IS NOT NULL ) 
   ) AS a -- This needs to be added
);

SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

LOOP
  SET idx = idx + 1;

  IF idx > idx_length THEN
    BREAK;
  END IF;

  SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
  SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

  SET difference = 
    CASE 
    WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
    WHEN expected_value = 0 and actual_value = 0 Then 0
    ELSE actual_value
    END;

  SET audit_status = 
  CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
  END;

  IF idx = 1 THEN
    SET audit_type = "RECORD_COUNT";
  ELSE
    SET audit_type = CONCAT("INGEST_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END