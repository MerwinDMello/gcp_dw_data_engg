DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_financial_transaction_dist.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Sean Wilson
       Date: 7/16/2014
       Name: CC_Financial_Transaction_Dist.sql
       Mod1: Added dagnostics per Teradata to avoid spool space errors SW.
       Mod2: Removed CAST on Patient Account number on 1/14/2015 AS.
       Mod3: Optimized SQL by adding join to ref_cc_org_strucutre to derive
             clinical_acctkeys on 2/26/2015 SW.
       Mod4: Added diagnostics to speed up query run-time on 3/26/2015 SW.
       Mod5: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance
             on 4/23/2015 SW.
       Mod6: Removed diagnostics and implemented the use of a volatile table
             to decrease run-time and spool on 5/19/2017 SW and PT.
	Mod7:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
*****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA232;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- /*Diagnostic noprodjoin on for session;
 -- Diagnostic nohashjoin on for session;
 -- Diagnostic noviewfold on for session;
 -- Diagnostic noparallel on for session; */ 
BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

CREATE
TEMPORARY TABLE vtl CLUSTER BY id,
                               schema_id AS
SELECT ma.account_no AS account_no,
       ma.id AS id,
       ma.schema_id AS schema_id,
       ma.org_id_provider AS org_id_provider,
       og.client_id AS og_client_id,
       og.short_name AS short_name,
       og.org_id AS org_id,
       og.schema_id AS og_schema_id
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON og.org_id = ma.org_id_provider
AND og.schema_id = ma.schema_id
WHERE DATE(ma.service_date_begin) >= DATE '2011-09-01';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;


CREATE
TEMPORARY TABLE vtl_z CLUSTER BY company_cd,
                                                                             coido,
                                                                             patient_dw_id,
                                                                             account_transaction_id AS
SELECT max(reforg.company_code) AS company_cd,
       max(CASE
               WHEN upper(rtrim(substr(ma.og_client_id, 1, 5))) = 'COID:' THEN substr(ma.og_client_id, 7, 5)
               ELSE ma.og_client_id
           END) AS coido,
       a.patient_dw_id,
       matd.mon_account_transaction_id AS account_transaction_id,
       matd.apl_transaction_category_id AS redistribution_category_id,
       CAST(matd.redistribution_date AS DATETIME) AS redistribution_date_time,
       max(ma.short_name) AS unit_num_o,
       ma.account_no AS pat_acct_nbr,
       max(usr.login_id) AS redistribution_user_id,
       matd.category_amount AS redistribution_amt
FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_transaction_dist AS matd
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_transaction AS mat ON matd.mon_account_transaction_id = mat.id
AND matd.schema_id = mat.schema_id
INNER JOIN vtl AS ma ON ma.id = mat.mon_account_id
AND ma.schema_id = mat.schema_id
INNER JOIN /*JOIN EDWRA_STAGING.Mon_Account MA ON
               MA.Id = MAT.Mon_Account_Id AND
               MA.Schema_Id = MAT.Schema_Id AND
               MA.Service_Date_Begin >= DATE '2011-09-01'
          JOIN EDWRA_STAGING.Org OG ON
               OG.Org_Id = MA.Org_Id_Provider AND
               OG.Schema_Id = MA.Schema_Id   */ {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON usr.user_id = matd.user_id_redistributed_by
AND usr.schema_id = matd.schema_id
INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(ma.og_client_id, 7, 5)))
AND reforg.schema_id = ma.og_schema_id
INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
AND a.pat_acct_num = ma.account_no
GROUP BY upper(reforg.company_code),
         upper(CASE
                   WHEN upper(rtrim(substr(ma.og_client_id, 1, 5))) = 'COID:' THEN substr(ma.og_client_id, 7, 5)
                   ELSE ma.og_client_id
               END),
         3,
         4,
         5,
         6,
         upper(ma.short_name),
         8,
         upper(usr.login_id),
         10;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- COLLECT STATISTICS is not supported in this dialect.
BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = 'edwra_staging.mon_account_transaction_dist';
SET tgttablename = 'edwra.cc_financial_transaction_dist';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from edwra_staging.mon_account_transaction_dist matd
join edwra_staging.mon_account_transaction mat on
     matd.mon_account_transaction_id = mat.id and
     matd.schema_id = mat.schema_id
join (select 
ma.account_no as account_no  ,
ma.id as id,
ma.schema_id as schema_id,
ma.org_id_provider as org_id_provider,
og.client_id as og_client_id,
og.short_name as short_name,
og.org_id as org_id,
og.schema_id as og_schema_id
 from  edwra_staging.mon_account ma 
join edwra_staging.org og on
   og.org_id = ma.org_id_provider and
   og.schema_id = ma.schema_id 
   where ma.service_date_begin >= date '2011-09-01'
) ma on
     ma.id = mat.mon_account_id and
     ma.schema_id = mat.schema_id       
join edwra_staging.users usr on
     usr.user_id = matd.user_id_redistributed_by and
     usr.schema_id = matd.schema_id
join edwra.ref_cc_org_structure reforg on
     reforg.coid = substr(ma.og_client_id,7,5) and
     reforg.schema_id = ma.og_schema_id
join edwra_staging.clinical_acctkeys a on 
     a.coid = reforg.coid and
     a.company_code = reforg.company_code and
     a.pat_acct_num = ma.account_no 
group by upper(reforg.company_code),
         upper(CASE
                   WHEN upper(rtrim(substr(ma.og_client_id, 1, 5))) = 'COID:' THEN substr(ma.og_client_id, 7, 5)
                   ELSE ma.og_client_id
               END),
         upper(ma.short_name),
         upper(usr.login_id)
               --,3,4,5,6,(upper(ma.short_name)),8,upper(usr.login_id),10 
);

SET actual_value =
(
select count(*) as row_count
from edwra.cc_financial_transaction_dist
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO edwra_ac.audit_control
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

BEGIN
SET _ERROR_CODE = 0;




MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction_dist AS x USING vtl_z AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_cd))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coido))
AND x.patient_dw_id = z.patient_dw_id
AND x.account_transaction_id = z.account_transaction_id
AND x.redistribution_category_id = z.redistribution_category_id
AND cast(x.redistribution_date_time as datetime) = cast(z.redistribution_date_time as DATETIME) WHEN MATCHED THEN
UPDATE
SET unit_num = substr(z.unit_num_o, 1, 5),
    pat_acct_num = z.pat_acct_nbr,
    redistribution_user_id = substr(z.redistribution_user_id, 1, 20),
    redistribution_amt = ROUND(z.redistribution_amt, 3, 'ROUND_HALF_EVEN'),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        account_transaction_id,
        redistribution_category_id,
        redistribution_date_time,
        unit_num,
        pat_acct_num,
        redistribution_user_id,
        redistribution_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_cd, substr(z.coido, 1, 5), z.patient_dw_id, z.account_transaction_id, z.redistribution_category_id, z.redistribution_date_time, substr(z.unit_num_o, 1, 5), z.pat_acct_nbr, substr(z.redistribution_user_id, 1, 20), ROUND(z.redistribution_amt, 3, 'ROUND_HALF_EVEN'), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             account_transaction_id,
             redistribution_category_id,
             redistribution_date_time
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_financial_transaction_dist
      GROUP BY company_code,
               coid,
               patient_dw_id,
               account_transaction_id,
               redistribution_category_id,
               redistribution_date_time
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.cc_financial_transaction_dist');
END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Financial_Transaction_Dist');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;