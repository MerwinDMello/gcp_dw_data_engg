DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_financial_transaction.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************************************************************
  Developer: Sean Wilson
       Date: 7/16/2014
       Name: CC_Financial_Transaction.sql
       Mod1: Added dagnostics per Teradata to avoid spool space errors SW.
       Mod2: Removed CAST on Patient Account number on 1/14/2015 AS.
       Mod3: Optimized SQL by adding additional join to Ref_CC_Org_Strucutre schema_id
             to derive clinical_acctkey on 2/26/2015 SW.
       Mod4: Added diagnostics to speed up query run-time on 3/26/2015 SW.
       Mod5: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
       Mod6: Changed to use EDWRA_STAGING.Payor_Organization to resolve duplicate issue on source system code on 08/08/2018 SW
	   Mod7: PBI14772: To address slow performance, Teradata DBAs have suggested a change to query band which will allow
	         setting priority for the job. 8/30/2018 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod9: Modified query to use volatile table as suggested by DBA as part of optimizaton on 7/3/2019 AM
        Mod10: Saravana Moorthy - Remove Diagnositic information, Added locks and Audit Merge query
*****************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA231;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

CREATE
TEMPORARY TABLE vtl_z CLUSTER BY company_cd,
                                 coido,
                                 patient_dw_id,
                                 account_transaction_id AS
SELECT max(a.company_code) AS company_cd,
       max(CASE
               WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
               ELSE og.client_id
           END) AS coido,
       a.patient_dw_id,
       mat.id AS account_transaction_id,
       max(og.short_name) AS unit_num,
       ma.account_no AS pat_acct_nbr,
       po.payor_dw_id,
       min(mapyr.payer_rank) AS iplan_insurance_order_num,
       CASE
           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
       END AS iplan,
       max(mat.misc_char01) AS transaction_type,
       mat.transaction_date AS transaction_enter_date_time,
       mat.effective_date AS transaction_eff_date_time,
       max(mtm.code) AS transaction_code,
       mat.amount AS transaction_amt,
       mat.misc_date01 AS transaction_bill_thru_date,
       max(mat.comments) AS transaction_comment_text,
       max(mat.misc_char02) AS icn_num,
       mat.snapshot_mon_status_id AS status_category_id,
       mat.snapshot_mon_reason_id AS reason_id,
       mat.mon_accounting_period_id AS financial_period_id,
       mat.snapshot_appeal_no AS appeal_num,
       mat.snapshot_appeal_seq_no AS appeal_seq_num,
       max(CASE
               WHEN mat.is_system_reversal = 1 THEN 'Y'
               ELSE 'N'
           END) AS reversal_ind,
       max(CASE
               WHEN mat.is_user_redistributed = 1 THEN 'Y'
               ELSE 'N'
           END) AS redistributed_ind,
       mat.parent_transaction_id AS parent_account_transaction_id,
       max(usr.login_id) AS transaction_create_user_id,
       mat.date_created AS transaction_create_date_time,
       max(usr2.login_id) AS transaction_update_user_id,
       mat.date_updated AS transaction_update_date_time
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_transaction AS mat
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mat.mon_account_id = ma.id
AND mat.schema_id = ma.schema_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_transaction_master AS mtm ON mat.mon_transaction_master_id = mtm.id
AND mat.schema_id = mtm.schema_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
AND ma.schema_id = og.schema_id
AND DATE(ma.service_date_begin) >= DATE '2011-09-01'
INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON og.org_id = ros.org_id
AND og.schema_id = ros.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_accounting_period AS mapd ON mat.mon_accounting_period_id = mapd.id
AND mat.schema_id = mapd.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mat.mon_payer_id = mpyr.id
AND mat.schema_id = mpyr.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mat.schema_id = mapyr.schema_id
AND mat.mon_account_id = mapyr.mon_account_id
AND mat.mon_payer_id = mapyr.mon_payer_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON mat.user_id_created_by = usr.user_id
AND mat.schema_id = usr.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr2 ON mat.user_id_updated_by = usr2.user_id
AND mat.schema_id = usr2.schema_id
LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.payor_organization AS po ON upper(rtrim(po.coid)) = upper(rtrim(ros.coid))
AND upper(rtrim(po.company_code)) = upper(rtrim(ros.company_code))
AND po.iplan_id = CASE
                      WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                      ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                  END
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(ros.coid))
AND upper(rtrim(a.company_code)) = upper(rtrim(ros.company_code))
AND a.pat_acct_num = ma.account_no
GROUP BY upper(a.company_code),
         upper(CASE
                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                   ELSE og.client_id
               END),
         3,
         4,
         upper(og.short_name),
         6,
         7,
         9,
         upper(mat.misc_char01),
         11,
         12,
         upper(mtm.code),
         14,
         15,
         upper(mat.comments),
         upper(mat.misc_char02),
         18,
         19,
         20,
         21,
         22,
         upper(CASE
                   WHEN mat.is_system_reversal = 1 THEN 'Y'
                   ELSE 'N'
               END),
         upper(CASE
                   WHEN mat.is_user_redistributed = 1 THEN 'Y'
                   ELSE 'N'
               END),
         25,
         upper(usr.login_id),
         27,
         upper(usr2.login_id),
         29;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction AS x USING vtl_z AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_cd))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coido))
AND x.patient_dw_id = z.patient_dw_id
AND x.account_transaction_id = z.account_transaction_id WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = z.pat_acct_nbr,
    payor_dw_id = z.payor_dw_id,
    iplan_insurance_order_num = CAST(z.iplan_insurance_order_num AS INT64),
    iplan_id = z.iplan,
    transaction_type = substr(z.transaction_type, 1, 1),
    transaction_enter_date_time = CAST(z.transaction_enter_date_time AS DATETIME),
    transaction_eff_date_time = CAST(z.transaction_eff_date_time AS DATETIME),
    transaction_code = substr(z.transaction_code, 1, 8),
    transaction_amt = ROUND(z.transaction_amt, 3, 'ROUND_HALF_EVEN'),
    transaction_bill_thru_date = z.transaction_bill_thru_date,
    transaction_comment_text = substr(z.transaction_comment_text, 1, 300),
    icn_num = substr(z.icn_num, 1, 25),
    status_category_id = z.status_category_id,
    reason_id = z.reason_id,
    financial_period_id = z.financial_period_id,
    appeal_num = z.appeal_num,
    appeal_seq_num = CAST(z.appeal_seq_num AS INT64),
    reversal_ind = substr(z.reversal_ind, 1, 1),
    redistributed_ind = substr(z.redistributed_ind, 1, 1),
    parent_account_transaction_id = z.parent_account_transaction_id,
    transaction_create_user_id = substr(z.transaction_create_user_id, 1, 20),
    transaction_create_date_time = CAST(z.transaction_create_date_time AS DATETIME),
    transaction_update_user_id = substr(z.transaction_update_user_id, 1, 20),
    transaction_update_date_time = CAST(z.transaction_update_date_time AS DATETIME),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        account_transaction_id,
        unit_num,
        pat_acct_num,
        payor_dw_id,
        iplan_insurance_order_num,
        iplan_id,
        transaction_type,
        transaction_enter_date_time,
        transaction_eff_date_time,
        transaction_code,
        transaction_amt,
        transaction_bill_thru_date,
        transaction_comment_text,
        icn_num,
        status_category_id,
        reason_id,
        financial_period_id,
        appeal_num,
        appeal_seq_num,
        reversal_ind,
        redistributed_ind,
        parent_account_transaction_id,
        transaction_create_user_id,
        transaction_create_date_time,
        transaction_update_user_id,
        transaction_update_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_cd, substr(z.coido, 1, 5), z.patient_dw_id, z.account_transaction_id, substr(z.unit_num, 1, 5), z.pat_acct_nbr, z.payor_dw_id, CAST(z.iplan_insurance_order_num AS INT64), z.iplan, substr(z.transaction_type, 1, 1), CAST(z.transaction_enter_date_time AS DATETIME), CAST(z.transaction_eff_date_time AS DATETIME), substr(z.transaction_code, 1, 8), ROUND(z.transaction_amt, 3, 'ROUND_HALF_EVEN'), z.transaction_bill_thru_date, substr(z.transaction_comment_text, 1, 300), substr(z.icn_num, 1, 25), z.status_category_id, z.reason_id, z.financial_period_id, z.appeal_num, CAST(z.appeal_seq_num AS INT64), substr(z.reversal_ind, 1, 1), substr(z.redistributed_ind, 1, 1), z.parent_account_transaction_id, substr(z.transaction_create_user_id, 1, 20), CAST(z.transaction_create_date_time AS DATETIME), substr(z.transaction_update_user_id, 1, 20), CAST(z.transaction_update_date_time AS DATETIME), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             account_transaction_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction
      GROUP BY company_code,
               coid,
               patient_dw_id,
               account_transaction_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.cc_financial_transaction');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Financial_Transaction');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;