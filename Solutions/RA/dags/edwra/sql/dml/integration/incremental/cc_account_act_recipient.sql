DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_account_act_recipient.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Holly Ray
          Date: 8/13/2011
          Name: CC_Account_Recipient_Build.sql
          Mod1: Replaces original CC_Account_Recipient_Build table due to revised Activity Data Model.
          Mod2: Another table revision from Architect Review. 09072011 HR
          Mod3: Changed path to DB Logon for DMExpress conversion on 2/12/2012 SW.
          Mod4: Added Diagnostics per Teradata for long running queries on 6/30/2014 FY.
	  Mod5: Removed CAST on Patient Account Number field. AS.
	  Mod6: Optimized SQL by using Ref_CC_Org_Strucutre to get Clinical_Acctkeys on 2/25/2015 SW.
	  Mod7: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod9:Added Audit Merge 11052020
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA217;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Diagnostic noprodjoin on for session;
 -- Diagnostic nohashjoin on for session;
 -- Diagnostic noviewfold on for session;
 BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg AS mt USING
  (SELECT DISTINCT a.patient_dw_id AS patient_dw_id,
                   maar.mon_account_activity_id AS activity_id,
                   maar.id AS activity_recipient_id,
                   reforg.company_code AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   substr(usr.login_id, 1, 20) AS recipient_login_userid,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_act_recipient AS maar
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maar.user_id_recipient = usr.user_id
   AND maar.schema_id = usr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity AS maa ON maar.mon_account_activity_id = maa.id
   AND maar.schema_id = maa.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON maa.mon_account_id = ma.id
   AND maa.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(CASE
                                                                                                                                    WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                    ELSE og.client_id
                                                                                                                                END, 1, 5)))
   AND reforg.schema_id = maar.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(substr(CASE
                                                                                                                               WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                               ELSE og.client_id
                                                                                                                           END, 1, 5)))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.activity_id, NUMERIC '0') = coalesce(ms.activity_id, NUMERIC '0')
     AND coalesce(mt.activity_id, NUMERIC '1') = coalesce(ms.activity_id, NUMERIC '1'))
AND (coalesce(mt.activity_recipient_id, NUMERIC '0') = coalesce(ms.activity_recipient_id, NUMERIC '0')
     AND coalesce(mt.activity_recipient_id, NUMERIC '1') = coalesce(ms.activity_recipient_id, NUMERIC '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_nbr, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_nbr, NUMERIC '1'))
AND (upper(coalesce(mt.recipient_login_userid, '0')) = upper(coalesce(ms.recipient_login_userid, '0'))
     AND upper(coalesce(mt.recipient_login_userid, '1')) = upper(coalesce(ms.recipient_login_userid, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        activity_id,
        activity_recipient_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        recipient_login_userid,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.patient_dw_id, ms.activity_id, ms.activity_recipient_id, ms.company_cd, ms.coido, ms.unit_num, ms.pat_acct_nbr, ms.recipient_login_userid, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             activity_id,
             activity_recipient_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg
      GROUP BY patient_dw_id,
               activity_id,
               activity_recipient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Account_Act_Recipient_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_account_act_recipient';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_act_recipient maar
      join {{ params.param_parallon_ra_stage_dataset_name }}.users usr
      on maar.user_id_recipient = usr.user_id and
      maar.schema_id  = usr.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity maa
      on maar.mon_account_activity_id = maa.id and
        maar.schema_id = maa.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account ma 
      on maa.mon_account_id = ma.id and
        maa.schema_id = ma.schema_id
       join {{ params.param_parallon_ra_stage_dataset_name }}.org og
       on ma.org_id_provider = og.org_id and
         ma.schema_id = og.schema_id
      join {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure reforg
         on reforg.coid = substr(CASE WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5) ELSE og.client_id END, 1, 5) and
                  reforg.schema_id = maar.schema_id
     join {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys a on
         a.coid = substr(CASE WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5) ELSE og.client_id END, 1, 5) and
         a.company_code = reforg.company_code and
         a.pat_acct_num = ma.account_no 
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg 
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


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient AS x USING
  (SELECT cc_account_act_recipient_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_act_recipient_stg) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.activity_id = z.activity_id
AND x.activity_recipient_id = z.activity_recipient_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = z.coid,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    recipient_login_userid = z.recipient_login_userid,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        activity_id,
        activity_recipient_id,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        recipient_login_userid,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.activity_id, z.activity_recipient_id, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.recipient_login_userid, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             activity_id,
             activity_recipient_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient
      GROUP BY patient_dw_id,
               activity_id,
               activity_recipient_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_act_recipient');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Account_Act_Recipient');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;