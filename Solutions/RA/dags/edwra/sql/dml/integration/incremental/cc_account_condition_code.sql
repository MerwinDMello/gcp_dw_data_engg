DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_account_condition_code.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_Account_Condition_Code - BTEQ Script.
      Mod1: Creation of script on 8/9/2011. SW.
      Mod2: Modifed script for new DDL on 9/6/2011. SW.
      Mod3: Modified script to filter null condition codes on 10/20/2011. HR
      Mod4: Modified script to use new tables for CC 9.7 on 3/6/2012 SW.
      Mod5: Added Diagnostics per Teradata for long running queries on 6/30/2014 FY.
      Mod6: Tuned SQL for performance, Removed Diagnostics on 11/5/2014 SW.
      Mod7: Removed CAST on Patient Account Number on 1/13/2014. AS
      Mod8: Added Diagnostics due to long running query on 3/26/2015 SW.
      Mod9: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	Mod10:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA224;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Diagnostic noprodjoin on for session;
 -- Diagnostic nohashjoin on for session;
 -- Diagnostic noviewfold on for session;
 BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_condition_code_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_condition_code_merge AS mt USING
  (SELECT DISTINCT a.patient_dw_id,
                   CAST(mancc.payer_rank AS INT64) AS condition_code_seq,
                   reforg.company_code AS company_cd,
                   substr(CASE
                              WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                              ELSE og.client_id
                          END, 1, 5) AS coido,
                   substr(og.short_name, 1, 5) AS unit_num,
                   ma.account_no AS pat_acct_nbr,
                   substr(mancc.code, 1, 2) AS condition_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_nonclinical_code AS mancc
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON ma.id = mancc.mon_account_id
   AND ma.schema_id = mancc.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.schema_id = og.schema_id
   AND ma.org_id_provider = og.org_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv ON mancc.code_category = pv.id
   AND mancc.schema_id = pv.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON reforg.org_id = og.org_id
   AND reforg.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no
   WHERE upper(rtrim(upper(pv.group_id))) = 'NONCLINICAL_CODE_CATEGORY'
     AND upper(rtrim(upper(pv.display_text))) = 'CONDITION CODES' ) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.condition_code_seq, 0) = coalesce(ms.condition_code_seq, 0)
     AND coalesce(mt.condition_code_seq, 1) = coalesce(ms.condition_code_seq, 1))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_nbr, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_nbr, NUMERIC '1'))
AND (upper(coalesce(mt.condition_code, '0')) = upper(coalesce(ms.condition_code, '0'))
     AND upper(coalesce(mt.condition_code, '1')) = upper(coalesce(ms.condition_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        condition_code_seq,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        condition_code,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.patient_dw_id, ms.condition_code_seq, ms.company_cd, ms.coido, ms.unit_num, ms.pat_acct_nbr, ms.condition_code, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             condition_code_seq
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_condition_code_merge
      GROUP BY patient_dw_id,
               condition_code_seq
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_condition_code_merge');

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

TRUNCATE TABLE {{ params.param_parallon_ra_core_dataset_name }}.cc_account_condition_code;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_condition_code AS x USING
  (SELECT cc_account_condition_code_merge.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_condition_code_merge) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.condition_code_seq = z.condition_code_seq WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = z.coid,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    condition_code = z.condition_code,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        condition_code_seq,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        condition_code,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.condition_code_seq, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.condition_code, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             condition_code_seq
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_condition_code
      GROUP BY patient_dw_id,
               condition_code_seq
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_condition_code');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_Account_Condition_Code');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;