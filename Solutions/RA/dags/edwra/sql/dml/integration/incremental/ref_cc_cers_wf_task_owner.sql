DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/ref_cc_cers_wf_task_owner.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************************
     Developer: Jason Chapman
          Date: 7/5/2015
          Name: Ref_CC_CERS_WF_Task_Owner.sql
         Mod1: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	Mod2:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
***********************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA282;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge AS mt USING
  (SELECT DISTINCT rccos.company_code AS company_code,
                   rccos.coid AS coid,
                   ROUND(cwto.id, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_owner_id,
                   ROUND(cwto.cers_workflow_task_id, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_id,
                   ROUND(cwto.user_id_owner, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_owner_uid,
                   cwto.owner_name AS cers_wf_task_owner_name,
                   cwto.owner_email AS cers_wf_task_owner_email_addr,
                   substr(CASE
                              WHEN cwto.is_required_to_complete = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_owner_cplt_ind,
                   substr(CASE
                              WHEN cwto.is_notified = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_owner_is_notf_ind,
                   cwto.completion_date AS cers_wf_task_ownr_cplt_date,
                   substr(CASE
                              WHEN cwto.checked_off_by_user_id = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_ownr_chck_off_ind,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task_owner AS cwto
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = cwto.schema_id) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.cers_wf_task_owner_id, NUMERIC '0') = coalesce(ms.cers_wf_task_owner_id, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_owner_id, NUMERIC '1') = coalesce(ms.cers_wf_task_owner_id, NUMERIC '1'))
AND (coalesce(mt.cers_wf_task_id, NUMERIC '0') = coalesce(ms.cers_wf_task_id, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_id, NUMERIC '1') = coalesce(ms.cers_wf_task_id, NUMERIC '1'))
AND (coalesce(mt.cers_wf_task_owner_uid, NUMERIC '0') = coalesce(ms.cers_wf_task_owner_uid, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_owner_uid, NUMERIC '1') = coalesce(ms.cers_wf_task_owner_uid, NUMERIC '1'))
AND (upper(coalesce(mt.cers_wf_task_owner_name, '0')) = upper(coalesce(ms.cers_wf_task_owner_name, '0'))
     AND upper(coalesce(mt.cers_wf_task_owner_name, '1')) = upper(coalesce(ms.cers_wf_task_owner_name, '1')))
AND (upper(coalesce(mt.cers_wf_task_owner_email_addr, '0')) = upper(coalesce(ms.cers_wf_task_owner_email_addr, '0'))
     AND upper(coalesce(mt.cers_wf_task_owner_email_addr, '1')) = upper(coalesce(ms.cers_wf_task_owner_email_addr, '1')))
AND (upper(coalesce(mt.cers_wf_task_owner_cplt_ind, '0')) = upper(coalesce(ms.cers_wf_task_owner_cplt_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_owner_cplt_ind, '1')) = upper(coalesce(ms.cers_wf_task_owner_cplt_ind, '1')))
AND (upper(coalesce(mt.cers_wf_task_owner_is_notf_ind, '0')) = upper(coalesce(ms.cers_wf_task_owner_is_notf_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_owner_is_notf_ind, '1')) = upper(coalesce(ms.cers_wf_task_owner_is_notf_ind, '1')))
AND (coalesce(mt.cers_wf_task_ownr_cplt_date, DATE '1970-01-01') = coalesce(ms.cers_wf_task_ownr_cplt_date, DATE '1970-01-01')
     AND coalesce(mt.cers_wf_task_ownr_cplt_date, DATE '1970-01-02') = coalesce(ms.cers_wf_task_ownr_cplt_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.cers_wf_task_ownr_chck_off_ind, '0')) = upper(coalesce(ms.cers_wf_task_ownr_chck_off_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_ownr_chck_off_ind, '1')) = upper(coalesce(ms.cers_wf_task_ownr_chck_off_ind, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_wf_task_owner_id,
        cers_wf_task_id,
        cers_wf_task_owner_uid,
        cers_wf_task_owner_name,
        cers_wf_task_owner_email_addr,
        cers_wf_task_owner_cplt_ind,
        cers_wf_task_owner_is_notf_ind,
        cers_wf_task_ownr_cplt_date,
        cers_wf_task_ownr_chck_off_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.company_code, ms.coid, ms.cers_wf_task_owner_id, ms.cers_wf_task_id, ms.cers_wf_task_owner_uid, ms.cers_wf_task_owner_name, ms.cers_wf_task_owner_email_addr, ms.cers_wf_task_owner_cplt_ind, ms.cers_wf_task_owner_is_notf_ind, ms.cers_wf_task_ownr_cplt_date, ms.cers_wf_task_ownr_chck_off_ind, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_wf_task_owner_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge
      GROUP BY company_code,
               coid,
               cers_wf_task_owner_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','Ref_CC_CERS_WF_Task_Owner_Merge');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task_owner';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge';
SET audit_type= 'RECORD_COUNT';

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task_owner cwto
  	join {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure rccos on
     		rccos.schema_id = cwto.schema_id
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge
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


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner AS x USING
  (SELECT ref_cc_cers_wf_task_owner_merge.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_owner_merge) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cers_wf_task_owner_id = z.cers_wf_task_owner_id WHEN MATCHED THEN
UPDATE
SET cers_wf_task_id = z.cers_wf_task_id,
    cers_wf_task_owner_uid = z.cers_wf_task_owner_uid,
    cers_wf_task_owner_name = z.cers_wf_task_owner_name,
    cers_wf_task_owner_email_addr = z.cers_wf_task_owner_email_addr,
    cers_wf_task_owner_cplt_ind = z.cers_wf_task_owner_cplt_ind,
    cers_wf_task_owner_is_notf_ind = z.cers_wf_task_owner_is_notf_ind,
    cers_wf_task_ownr_cplt_date = z.cers_wf_task_ownr_cplt_date,
    cers_wf_task_ownr_chck_off_ind = z.cers_wf_task_ownr_chck_off_ind,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_wf_task_owner_id,
        cers_wf_task_id,
        cers_wf_task_owner_uid,
        cers_wf_task_owner_name,
        cers_wf_task_owner_email_addr,
        cers_wf_task_owner_cplt_ind,
        cers_wf_task_owner_is_notf_ind,
        cers_wf_task_ownr_cplt_date,
        cers_wf_task_ownr_chck_off_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.cers_wf_task_owner_id, z.cers_wf_task_id, z.cers_wf_task_owner_uid, z.cers_wf_task_owner_name, z.cers_wf_task_owner_email_addr, z.cers_wf_task_owner_cplt_ind, z.cers_wf_task_owner_is_notf_ind, z.cers_wf_task_ownr_cplt_date, z.cers_wf_task_ownr_chck_off_ind, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_wf_task_owner_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner
      GROUP BY company_code,
               coid,
               cers_wf_task_owner_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner
WHERE ref_cc_cers_wf_task_owner.dw_last_update_date_time <>
    (SELECT max(ref_cc_cers_wf_task_owner_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task_owner AS ref_cc_cers_wf_task_owner_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','Ref_CC_CERS_WF_Task_Owner');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;