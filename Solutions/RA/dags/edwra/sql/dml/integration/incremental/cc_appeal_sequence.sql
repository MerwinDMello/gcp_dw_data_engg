DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_appeal_sequence.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**************************************************************************************
   Developer: Sean Wilson
        Date: 2/16/2011
        Name: CC_Appeal_Sequence.sql
        Mod1: Initial creation of BTEQ script on 2/15/2011.
        Mod2: Script changed due to new Appeals model on 9/26/2011. SW.
        Mod3: Cleaned up code for JOIN conditions on 9/18/2013 SW.
  	    Mod4: Removed CAST on Patient Account number on 1/14/2015 AS.
	    Mod5: More optimization by adding join to ref_cc_org_structure on 2/25/2015 SW.
	    Mod6: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	    Mod7: Added Vendor_Code and Reopen Appeal logic on 10/4/2016 SW.
		Mod8: Changed delete to only consider active coids on 1/31/2018 SW.
Mod9:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
Mod10: Added logic for 3 new columns Appeal_Level_Num,Appeal_Sent_Date,Prior_Appeal_Response_Date PT 1/9/2019
Mod11:  -  PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
Mod12: Added Audit Merge and removed EXP task from job 07222020 PT
****************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA230;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg AS mt USING
  (SELECT DISTINCT po.company_code AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   a.patient_dw_id,
                   po.payor_dw_id,
                   CAST(mapl.payer_rank AS INT64) AS iplan_insurance_order_num,
                   ROUND(mapl.appeal_no, 0, 'ROUND_HALF_EVEN') AS appeal_num,
                   CAST(maps.sequence_no AS INT64) AS appeal_seq_num,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64) AS iplan,
                   ROUND(maps.appeal_balance_amt_begin, 3, 'ROUND_HALF_EVEN') AS appeal_seq_begin_bal_amt,
                   ROUND(maps.appeal_balance_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_current_bal_amt,
                   ROUND(maps.appeal_balance_end_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_end_bal_amt,
                   maps.deadline_date AS appeal_seq_deadline_date,
                   CAST(maps.sequence_close_date AS DATETIME) AS appeal_seq_close_date_time,
                   maps.apl_root_cause_id AS appeal_seq_root_cause_id,
                   trim(maps.root_cause_detail) AS appeal_seq_root_cause_dtl_text,
                   maps.apl_disposition_id AS appeal_disp_code_id,
                   maps.apl_appeal_id AS appeal_code_id,
                   substr(usr.login_id, 1, 20) AS appeal_seq_owner_user_id,
                   substr(usr2.login_id, 1, 20) AS appeal_seq_create_user_id,
                   CAST(maps.date_created AS DATETIME) AS appeal_seq_create_date_time,
                   substr(usr3.login_id, 1, 20) AS appeal_seq_update_user_id,
                   CAST(maps.date_modified AS DATETIME) AS appeal_seq_update_date_time,
                   substr(usr4.login_id, 1, 20) AS appeal_disp_id_update_user_id,
                   CAST(maps.disposition_code_modified_date AS DATETIME) AS appeal_disp_id_date_time,
                   ven.code AS vendor_cd,
                   CAST(maps.reopen_date AS DATETIME) AS appeal_seq_reopen_date_time,
                   substr(maps.reopen_user, 1, 20) AS appeal_seq_reopen_user_id,
                   CAST(maps.apl_lvl AS INT64) AS appeal_level_num,
                   maps.apl_sent_dt AS appeal_sent_date,
                   maps.prior_apl_rspn_dt AS prior_appeal_response_date,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code,
                   maps.appeal_receipt_date AS appeal_receipt_date
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS maps
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS mapl ON maps.mon_appeal_id = mapl.id
   AND maps.schema_id = mapl.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapl.mon_payer_id = mpyr.id
   AND mapl.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapl.mon_account_id = ma.id
   AND mapl.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_id = mapyr.mon_account_id
   AND mapl.payer_rank = mapyr.payer_rank
   AND mapl.mon_payer_id = mapyr.mon_payer_id
   AND mapl.schema_id = mapyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maps.sequence_owner_id = usr.user_id
   AND maps.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON maps.user_id_created_by = usr2.user_id
   AND maps.schema_id = usr2.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr3 ON maps.user_id_modified_by = usr3.user_id
   AND maps.schema_id = usr3.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr4 ON maps.disposition_code_modified_by = usr4.user_id
   AND maps.schema_id = usr4.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.vendor AS ven ON ven.vendor_id = maps.vendor_id
   AND ven.schema_id = maps.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64)
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(mpyr.code)) NOT IN('000-00',
                                        'NO INS') ) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.appeal_num, NUMERIC '0') = coalesce(ms.appeal_num, NUMERIC '0')
     AND coalesce(mt.appeal_num, NUMERIC '1') = coalesce(ms.appeal_num, NUMERIC '1'))
AND (coalesce(mt.appeal_seq_num, 0) = coalesce(ms.appeal_seq_num, 0)
     AND coalesce(mt.appeal_seq_num, 1) = coalesce(ms.appeal_seq_num, 1))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_nbr, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_nbr, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan, 1))
AND (coalesce(mt.appeal_seq_begin_bal_amt, NUMERIC '0') = coalesce(ms.appeal_seq_begin_bal_amt, NUMERIC '0')
     AND coalesce(mt.appeal_seq_begin_bal_amt, NUMERIC '1') = coalesce(ms.appeal_seq_begin_bal_amt, NUMERIC '1'))
AND (coalesce(mt.appeal_seq_current_bal_amt, NUMERIC '0') = coalesce(ms.appeal_seq_current_bal_amt, NUMERIC '0')
     AND coalesce(mt.appeal_seq_current_bal_amt, NUMERIC '1') = coalesce(ms.appeal_seq_current_bal_amt, NUMERIC '1'))
AND (coalesce(mt.appeal_seq_end_bal_amt, NUMERIC '0') = coalesce(ms.appeal_seq_end_bal_amt, NUMERIC '0')
     AND coalesce(mt.appeal_seq_end_bal_amt, NUMERIC '1') = coalesce(ms.appeal_seq_end_bal_amt, NUMERIC '1'))
AND (coalesce(mt.appeal_seq_deadline_date, DATE '1970-01-01') = coalesce(ms.appeal_seq_deadline_date, DATE '1970-01-01')
     AND coalesce(mt.appeal_seq_deadline_date, DATE '1970-01-02') = coalesce(ms.appeal_seq_deadline_date, DATE '1970-01-02'))
AND (coalesce(mt.appeal_seq_close_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appeal_seq_close_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.appeal_seq_close_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appeal_seq_close_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.appeal_seq_root_cause_id, NUMERIC '0') = coalesce(ms.appeal_seq_root_cause_id, NUMERIC '0')
     AND coalesce(mt.appeal_seq_root_cause_id, NUMERIC '1') = coalesce(ms.appeal_seq_root_cause_id, NUMERIC '1'))
AND (upper(coalesce(mt.appeal_seq_root_cause_dtl_text, '0')) = upper(coalesce(ms.appeal_seq_root_cause_dtl_text, '0'))
     AND upper(coalesce(mt.appeal_seq_root_cause_dtl_text, '1')) = upper(coalesce(ms.appeal_seq_root_cause_dtl_text, '1')))
AND (coalesce(mt.appeal_disp_code_id, NUMERIC '0') = coalesce(ms.appeal_disp_code_id, NUMERIC '0')
     AND coalesce(mt.appeal_disp_code_id, NUMERIC '1') = coalesce(ms.appeal_disp_code_id, NUMERIC '1'))
AND (coalesce(mt.appeal_code_id, NUMERIC '0') = coalesce(ms.appeal_code_id, NUMERIC '0')
     AND coalesce(mt.appeal_code_id, NUMERIC '1') = coalesce(ms.appeal_code_id, NUMERIC '1'))
AND (upper(coalesce(mt.appeal_seq_owner_user_id, '0')) = upper(coalesce(ms.appeal_seq_owner_user_id, '0'))
     AND upper(coalesce(mt.appeal_seq_owner_user_id, '1')) = upper(coalesce(ms.appeal_seq_owner_user_id, '1')))
AND (upper(coalesce(mt.appeal_seq_create_user_id, '0')) = upper(coalesce(ms.appeal_seq_create_user_id, '0'))
     AND upper(coalesce(mt.appeal_seq_create_user_id, '1')) = upper(coalesce(ms.appeal_seq_create_user_id, '1')))
AND (coalesce(mt.appeal_seq_create_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appeal_seq_create_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.appeal_seq_create_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appeal_seq_create_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.appeal_seq_update_user_id, '0')) = upper(coalesce(ms.appeal_seq_update_user_id, '0'))
     AND upper(coalesce(mt.appeal_seq_update_user_id, '1')) = upper(coalesce(ms.appeal_seq_update_user_id, '1')))
AND (coalesce(mt.appeal_seq_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appeal_seq_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.appeal_seq_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appeal_seq_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.appeal_disp_id_update_user_id, '0')) = upper(coalesce(ms.appeal_disp_id_update_user_id, '0'))
     AND upper(coalesce(mt.appeal_disp_id_update_user_id, '1')) = upper(coalesce(ms.appeal_disp_id_update_user_id, '1')))
AND (coalesce(mt.appeal_disp_id_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appeal_disp_id_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.appeal_disp_id_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appeal_disp_id_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.vendor_cd, '0')) = upper(coalesce(ms.vendor_cd, '0'))
     AND upper(coalesce(mt.vendor_cd, '1')) = upper(coalesce(ms.vendor_cd, '1')))
AND (coalesce(mt.appeal_seq_reopen_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.appeal_seq_reopen_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.appeal_seq_reopen_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.appeal_seq_reopen_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.appeal_seq_reopen_user_id, '0')) = upper(coalesce(ms.appeal_seq_reopen_user_id, '0'))
     AND upper(coalesce(mt.appeal_seq_reopen_user_id, '1')) = upper(coalesce(ms.appeal_seq_reopen_user_id, '1')))
AND (coalesce(mt.appeal_level_num, 0) = coalesce(ms.appeal_level_num, 0)
     AND coalesce(mt.appeal_level_num, 1) = coalesce(ms.appeal_level_num, 1))
AND (coalesce(mt.appeal_sent_date, DATE '1970-01-01') = coalesce(ms.appeal_sent_date, DATE '1970-01-01')
     AND coalesce(mt.appeal_sent_date, DATE '1970-01-02') = coalesce(ms.appeal_sent_date, DATE '1970-01-02'))
AND (coalesce(mt.prior_appeal_response_date, DATE '1970-01-01') = coalesce(ms.prior_appeal_response_date, DATE '1970-01-01')
     AND coalesce(mt.prior_appeal_response_date, DATE '1970-01-02') = coalesce(ms.prior_appeal_response_date, DATE '1970-01-02'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.appeal_receipt_date, DATE '1970-01-01') = coalesce(ms.appeal_receipt_date, DATE '1970-01-01')
     AND coalesce(mt.appeal_receipt_date, DATE '1970-01-02') = coalesce(ms.appeal_receipt_date, DATE '1970-01-02'))
      WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        appeal_num,
        appeal_seq_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        appeal_seq_begin_bal_amt,
        appeal_seq_current_bal_amt,
        appeal_seq_end_bal_amt,
        appeal_seq_deadline_date,
        appeal_seq_close_date_time,
        appeal_seq_root_cause_id,
        appeal_seq_root_cause_dtl_text,
        appeal_disp_code_id,
        appeal_code_id,
        appeal_seq_owner_user_id,
        appeal_seq_create_user_id,
        appeal_seq_create_date_time,
        appeal_seq_update_user_id,
        appeal_seq_update_date_time,
        appeal_disp_id_update_user_id,
        appeal_disp_id_date_time,
        vendor_cd,
        appeal_seq_reopen_date_time,
        appeal_seq_reopen_user_id,
        appeal_level_num,
        appeal_sent_date,
        prior_appeal_response_date,
        dw_last_update_date_time,
        source_system_code,
        appeal_receipt_date)
VALUES (ms.company_cd, ms.coido, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.appeal_num, ms.appeal_seq_num, ms.unit_num, ms.pat_acct_nbr, ms.iplan, ms.appeal_seq_begin_bal_amt, ms.appeal_seq_current_bal_amt, ms.appeal_seq_end_bal_amt, ms.appeal_seq_deadline_date, ms.appeal_seq_close_date_time, ms.appeal_seq_root_cause_id, ms.appeal_seq_root_cause_dtl_text, ms.appeal_disp_code_id, ms.appeal_code_id, ms.appeal_seq_owner_user_id, ms.appeal_seq_create_user_id, ms.appeal_seq_create_date_time, ms.appeal_seq_update_user_id, ms.appeal_seq_update_date_time, ms.appeal_disp_id_update_user_id, ms.appeal_disp_id_date_time, ms.vendor_cd, ms.appeal_seq_reopen_date_time, ms.appeal_seq_reopen_user_id, ms.appeal_level_num, ms.appeal_sent_date, ms.prior_appeal_response_date, ms.dw_last_update_date_time, ms.source_system_code,ms.appeal_receipt_date);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             appeal_num,
             appeal_seq_num
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               appeal_num,
               appeal_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Appeal_Sequence_STG');
 --  audit merge -- 7/20/2020
BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence';
SET tgttablename = '{{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) FROM (SELECT DISTINCT po.company_code AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   a.patient_dw_id,
                   po.payor_dw_id,
                   CAST(mapl.payer_rank AS INT64) AS iplan_insurance_order_num,
                   ROUND(mapl.appeal_no, 0, 'ROUND_HALF_EVEN') AS appeal_num,
                   CAST(maps.sequence_no AS INT64) AS appeal_seq_num,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64) AS iplan,
                   ROUND(maps.appeal_balance_amt_begin, 3, 'ROUND_HALF_EVEN') AS appeal_seq_begin_bal_amt,
                   ROUND(maps.appeal_balance_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_current_bal_amt,
                   ROUND(maps.appeal_balance_end_amt, 3, 'ROUND_HALF_EVEN') AS appeal_seq_end_bal_amt,
                   maps.deadline_date AS appeal_seq_deadline_date,
                   CAST(maps.sequence_close_date AS DATETIME) AS appeal_seq_close_date_time,
                   maps.apl_root_cause_id AS appeal_seq_root_cause_id,
                   maps.root_cause_detail AS appeal_seq_root_cause_dtl_text,
                   maps.apl_disposition_id AS appeal_disp_code_id,
                   maps.apl_appeal_id AS appeal_code_id,
                   substr(usr.login_id, 1, 20) AS appeal_seq_owner_user_id,
                   substr(usr2.login_id, 1, 20) AS appeal_seq_create_user_id,
                   CAST(maps.date_created AS DATETIME) AS appeal_seq_create_date_time,
                   substr(usr3.login_id, 1, 20) AS appeal_seq_update_user_id,
                   CAST(maps.date_modified AS DATETIME) AS appeal_seq_update_date_time,
                   substr(usr4.login_id, 1, 20) AS appeal_disp_id_update_user_id,
                   CAST(maps.disposition_code_modified_date AS DATETIME) AS appeal_disp_id_date_time,
                   ven.code AS vendor_cd,
                   CAST(maps.reopen_date AS DATETIME) AS appeal_seq_reopen_date_time,
                   substr(maps.reopen_user, 1, 20) AS appeal_seq_reopen_user_id,
                   CAST(maps.apl_lvl AS INT64) AS appeal_level_num,
                   maps.apl_sent_dt AS appeal_sent_date,
                   maps.prior_apl_rspn_dt AS prior_appeal_response_date,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code,
                   appeal_receipt_date AS appeal_receipt_date
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal_sequence AS maps
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_appeal AS mapl ON maps.mon_appeal_id = mapl.id
   AND maps.schema_id = mapl.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapl.mon_payer_id = mpyr.id
   AND mapl.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapl.mon_account_id = ma.id
   AND mapl.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND rccos.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_id = mapyr.mon_account_id
   AND mapl.payer_rank = mapyr.payer_rank
   AND mapl.mon_payer_id = mapyr.mon_payer_id
   AND mapl.schema_id = mapyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maps.sequence_owner_id = usr.user_id
   AND maps.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON maps.user_id_created_by = usr2.user_id
   AND maps.schema_id = usr2.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr3 ON maps.user_id_modified_by = usr3.user_id
   AND maps.schema_id = usr3.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr4 ON maps.disposition_code_modified_by = usr4.user_id
   AND maps.schema_id = usr4.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.vendor AS ven ON ven.vendor_id = maps.vendor_id
   AND ven.schema_id = maps.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS po ON upper(rtrim(po.coid)) = upper(rtrim(rccos.coid))
   AND po.iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(mpyr.code, 1, 3), substr(mpyr.code, 5, 2))) AS INT64)
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rccos.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(mpyr.code)) NOT IN('000-00',
                                        'NO INS') )
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
(uuid, table_id, src_sys_nm, src_tbl_nm, tgt_tbl_nm, audit_type, 
expected_value, actual_value, load_start_time, load_end_time, 
load_run_time, job_name, audit_time, audit_status)
VALUES
(GENERATE_UUID(), cast(srctableid as int64), 'ra',srctablename, tgttablename, audit_type,
expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time, job_name, audit_time, audit_status
);

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence AS x USING
  (SELECT cc_appeal_sequence_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_appeal_sequence_stg) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.appeal_num = z.appeal_num
AND x.appeal_seq_num = z.appeal_seq_num WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    appeal_seq_begin_bal_amt = z.appeal_seq_begin_bal_amt,
    appeal_seq_current_bal_amt = z.appeal_seq_current_bal_amt,
    appeal_seq_end_bal_amt = z.appeal_seq_end_bal_amt,
    appeal_seq_deadline_date = z.appeal_seq_deadline_date,
    appeal_seq_close_date_time = z.appeal_seq_close_date_time,
    appeal_seq_root_cause_id = z.appeal_seq_root_cause_id,
    appeal_seq_root_cause_dtl_text = z.appeal_seq_root_cause_dtl_text,
    appeal_disp_code_id = z.appeal_disp_code_id,
    appeal_code_id = z.appeal_code_id,
    appeal_seq_owner_user_id = z.appeal_seq_owner_user_id,
    appeal_seq_create_user_id = z.appeal_seq_create_user_id,
    appeal_seq_create_date_time = z.appeal_seq_create_date_time,
    appeal_seq_update_user_id = z.appeal_seq_update_user_id,
    appeal_seq_update_date_time = z.appeal_seq_update_date_time,
    appeal_disp_id_update_user_id = z.appeal_disp_id_update_user_id,
    appeal_disp_id_date_time = z.appeal_disp_id_date_time,
    vendor_cd = z.vendor_cd,
    appeal_seq_reopen_date_time = z.appeal_seq_reopen_date_time,
    appeal_seq_reopen_user_id = z.appeal_seq_reopen_user_id,
    appeal_level_num = z.appeal_level_num,
    appeal_sent_date = z.appeal_sent_date,
    prior_appeal_response_date = z.prior_appeal_response_date,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N',
    appeal_receipt_date = z.appeal_receipt_date WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        appeal_num,
        appeal_seq_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        appeal_seq_begin_bal_amt,
        appeal_seq_current_bal_amt,
        appeal_seq_end_bal_amt,
        appeal_seq_deadline_date,
        appeal_seq_close_date_time,
        appeal_seq_root_cause_id,
        appeal_seq_root_cause_dtl_text,
        appeal_disp_code_id,
        appeal_code_id,
        appeal_seq_owner_user_id,
        appeal_seq_create_user_id,
        appeal_seq_create_date_time,
        appeal_seq_update_user_id,
        appeal_seq_update_date_time,
        appeal_disp_id_update_user_id,
        appeal_disp_id_date_time,
        vendor_cd,
        appeal_seq_reopen_date_time,
        appeal_seq_reopen_user_id,
        appeal_level_num,
        appeal_sent_date,
        prior_appeal_response_date,
        dw_last_update_date_time,
        source_system_code,
        appeal_receipt_date)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.iplan_insurance_order_num, z.appeal_num, z.appeal_seq_num, z.unit_num, z.pat_acct_num, z.iplan_id, z.appeal_seq_begin_bal_amt, z.appeal_seq_current_bal_amt, z.appeal_seq_end_bal_amt, z.appeal_seq_deadline_date, z.appeal_seq_close_date_time, z.appeal_seq_root_cause_id, z.appeal_seq_root_cause_dtl_text, z.appeal_disp_code_id, z.appeal_code_id, z.appeal_seq_owner_user_id, z.appeal_seq_create_user_id, z.appeal_seq_create_date_time, z.appeal_seq_update_user_id, z.appeal_seq_update_date_time, z.appeal_disp_id_update_user_id, z.appeal_disp_id_date_time, z.vendor_cd, z.appeal_seq_reopen_date_time, z.appeal_seq_reopen_user_id, z.appeal_level_num, z.appeal_sent_date, z.prior_appeal_response_date, datetime_trunc(current_datetime('US/Central'), SECOND), 'N',z.appeal_receipt_date);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             appeal_num,
             appeal_seq_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               appeal_num,
               appeal_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence
WHERE upper(cc_appeal_sequence.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND (upper(cc_appeal_sequence.company_code),
       upper(cc_appeal_sequence.coid),
       cc_appeal_sequence.patient_dw_id,
       cc_appeal_sequence.payor_dw_id,
       cc_appeal_sequence.iplan_insurance_order_num,
       cc_appeal_sequence.appeal_num,
       cc_appeal_sequence.appeal_seq_num) IN
    (SELECT AS STRUCT upper(cas.company_code) AS company_code,
                      upper(cas.coid) AS coid,
                      cas.patient_dw_id,
                      cas.payor_dw_id,
                      cas.iplan_insurance_order_num,
                      cas.appeal_num,
                      cas.appeal_seq_num
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal_sequence AS cas
     LEFT OUTER JOIN {{ params.param_parallon_ra_core_dataset_name }}.cc_appeal AS ca ON upper(rtrim(cas.company_code)) = upper(rtrim(ca.company_code))
     AND upper(rtrim(cas.coid)) = upper(rtrim(ca.coid))
     AND cas.patient_dw_id = ca.patient_dw_id
     AND cas.payor_dw_id = ca.payor_dw_id
     AND cas.iplan_insurance_order_num = ca.iplan_insurance_order_num
     AND cas.appeal_num = ca.appeal_num
     WHERE ca.appeal_num IS NULL );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Appeal_Sequence');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;