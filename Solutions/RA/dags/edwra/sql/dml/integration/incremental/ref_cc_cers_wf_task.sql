DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/ref_cc_cers_wf_task.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/**********************************************************************************************************************************
     Developer: Jason Chapman
          Date: 7/5/2015
          Name: Ref_CC_CERS_WF_Task.sql
		  Mod1: Add CE_WF_Profile_Id for Contract Modeling report  -  08/12/2015  jac
		  Mod2: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	Mod3:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
Mod4: Added Audit Merge 06302021
***********************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA281;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge AS mt USING
  (SELECT DISTINCT trim(rccos.company_code) AS company_code,
                   trim(rccos.coid) AS coid,
                   ROUND(cwt.id, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_id,
                   ROUND(cwt.cers_term_id, 0, 'ROUND_HALF_EVEN') AS cers_term_id,
                   ROUND(cwt.ce_workflow_profile_id, 0, 'ROUND_HALF_EVEN') AS ce_wf_profile_id,
                   cwp.name AS ce_wf_profile_name,
                   ROUND(cwt.task_sequence, 0, 'ROUND_HALF_EVEN') AS cers_task_sequence,
                   ROUND(cwt.ce_workflow_task_id, 0, 'ROUND_HALF_EVEN') AS ce_wf_task_id,
                   trim(cwt.name) AS cers_wf_task_name,
                   trim(cwt.description) AS cers_wf_task_desc,
                   ROUND(cwt.expected_duration, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_expected_dur_rate,
                   substr(CASE
                              WHEN cwt.is_attachment_required = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_is_atch_reqr_ind,
                   cwt.start_date AS cers_wf_task_start_date,
                   cwt.completion_date AS cers_wf_task_completed_date,
                   substr(CASE
                              WHEN cwt.is_active = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_is_active_ind,
                   trim(cwt.comments) AS cers_wf_task_comments,
                   substr(CASE
                              WHEN cwt.is_current = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS cers_wf_task_is_current_ind,
                   ROUND(cwt.user_id_created_by, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_create_uid,
                   cwt.date_created AS cers_wf_task_create_date,
                   ROUND(cwt.user_id_completed_by, 0, 'ROUND_HALF_EVEN') AS cers_wf_task_completed_by_uid,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task AS cwt
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ce_workflow_profile AS cwp ON cwt.ce_workflow_profile_id = cwp.id
   AND cwt.schema_id = cwp.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = cwt.schema_id) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.cers_wf_task_id, NUMERIC '0') = coalesce(ms.cers_wf_task_id, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_id, NUMERIC '1') = coalesce(ms.cers_wf_task_id, NUMERIC '1'))
AND (coalesce(mt.cers_term_id, NUMERIC '0') = coalesce(ms.cers_term_id, NUMERIC '0')
     AND coalesce(mt.cers_term_id, NUMERIC '1') = coalesce(ms.cers_term_id, NUMERIC '1'))
AND (coalesce(mt.ce_wf_profile_id, NUMERIC '0') = coalesce(ms.ce_wf_profile_id, NUMERIC '0')
     AND coalesce(mt.ce_wf_profile_id, NUMERIC '1') = coalesce(ms.ce_wf_profile_id, NUMERIC '1'))
AND (upper(coalesce(mt.ce_wf_profile_name, '0')) = upper(coalesce(ms.ce_wf_profile_name, '0'))
     AND upper(coalesce(mt.ce_wf_profile_name, '1')) = upper(coalesce(ms.ce_wf_profile_name, '1')))
AND (coalesce(mt.cers_task_sequence, NUMERIC '0') = coalesce(ms.cers_task_sequence, NUMERIC '0')
     AND coalesce(mt.cers_task_sequence, NUMERIC '1') = coalesce(ms.cers_task_sequence, NUMERIC '1'))
AND (coalesce(mt.ce_wf_task_id, NUMERIC '0') = coalesce(ms.ce_wf_task_id, NUMERIC '0')
     AND coalesce(mt.ce_wf_task_id, NUMERIC '1') = coalesce(ms.ce_wf_task_id, NUMERIC '1'))
AND (upper(coalesce(mt.cers_wf_task_name, '0')) = upper(coalesce(ms.cers_wf_task_name, '0'))
     AND upper(coalesce(mt.cers_wf_task_name, '1')) = upper(coalesce(ms.cers_wf_task_name, '1')))
AND (upper(coalesce(mt.cers_wf_task_desc, '0')) = upper(coalesce(ms.cers_wf_task_desc, '0'))
     AND upper(coalesce(mt.cers_wf_task_desc, '1')) = upper(coalesce(ms.cers_wf_task_desc, '1')))
AND (coalesce(mt.cers_wf_task_expected_dur_rate, NUMERIC '0') = coalesce(ms.cers_wf_task_expected_dur_rate, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_expected_dur_rate, NUMERIC '1') = coalesce(ms.cers_wf_task_expected_dur_rate, NUMERIC '1'))
AND (upper(coalesce(mt.cers_wf_task_is_atch_reqr_ind, '0')) = upper(coalesce(ms.cers_wf_task_is_atch_reqr_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_is_atch_reqr_ind, '1')) = upper(coalesce(ms.cers_wf_task_is_atch_reqr_ind, '1')))
AND (coalesce(mt.cers_wf_task_start_date, DATE '1970-01-01') = coalesce(ms.cers_wf_task_start_date, DATE '1970-01-01')
     AND coalesce(mt.cers_wf_task_start_date, DATE '1970-01-02') = coalesce(ms.cers_wf_task_start_date, DATE '1970-01-02'))
AND (coalesce(mt.cers_wf_task_completed_date, DATE '1970-01-01') = coalesce(ms.cers_wf_task_completed_date, DATE '1970-01-01')
     AND coalesce(mt.cers_wf_task_completed_date, DATE '1970-01-02') = coalesce(ms.cers_wf_task_completed_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.cers_wf_task_is_active_ind, '0')) = upper(coalesce(ms.cers_wf_task_is_active_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_is_active_ind, '1')) = upper(coalesce(ms.cers_wf_task_is_active_ind, '1')))
AND (upper(coalesce(mt.cers_wf_task_comments, '0')) = upper(coalesce(ms.cers_wf_task_comments, '0'))
     AND upper(coalesce(mt.cers_wf_task_comments, '1')) = upper(coalesce(ms.cers_wf_task_comments, '1')))
AND (upper(coalesce(mt.cers_wf_task_is_current_ind, '0')) = upper(coalesce(ms.cers_wf_task_is_current_ind, '0'))
     AND upper(coalesce(mt.cers_wf_task_is_current_ind, '1')) = upper(coalesce(ms.cers_wf_task_is_current_ind, '1')))
AND (coalesce(mt.cers_wf_task_create_uid, NUMERIC '0') = coalesce(ms.cers_wf_task_create_uid, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_create_uid, NUMERIC '1') = coalesce(ms.cers_wf_task_create_uid, NUMERIC '1'))
AND (coalesce(mt.cers_wf_task_create_date, DATE '1970-01-01') = coalesce(ms.cers_wf_task_create_date, DATE '1970-01-01')
     AND coalesce(mt.cers_wf_task_create_date, DATE '1970-01-02') = coalesce(ms.cers_wf_task_create_date, DATE '1970-01-02'))
AND (coalesce(mt.cers_wf_task_completed_by_uid, NUMERIC '0') = coalesce(ms.cers_wf_task_completed_by_uid, NUMERIC '0')
     AND coalesce(mt.cers_wf_task_completed_by_uid, NUMERIC '1') = coalesce(ms.cers_wf_task_completed_by_uid, NUMERIC '1'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_wf_task_id,
        cers_term_id,
        ce_wf_profile_id,
        ce_wf_profile_name,
        cers_task_sequence,
        ce_wf_task_id,
        cers_wf_task_name,
        cers_wf_task_desc,
        cers_wf_task_expected_dur_rate,
        cers_wf_task_is_atch_reqr_ind,
        cers_wf_task_start_date,
        cers_wf_task_completed_date,
        cers_wf_task_is_active_ind,
        cers_wf_task_comments,
        cers_wf_task_is_current_ind,
        cers_wf_task_create_uid,
        cers_wf_task_create_date,
        cers_wf_task_completed_by_uid,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.company_code, ms.coid, ms.cers_wf_task_id, ms.cers_term_id, ms.ce_wf_profile_id, ms.ce_wf_profile_name, ms.cers_task_sequence, ms.ce_wf_task_id, ms.cers_wf_task_name, ms.cers_wf_task_desc, ms.cers_wf_task_expected_dur_rate, ms.cers_wf_task_is_atch_reqr_ind, ms.cers_wf_task_start_date, ms.cers_wf_task_completed_date, ms.cers_wf_task_is_active_ind, ms.cers_wf_task_comments, ms.cers_wf_task_is_current_ind, ms.cers_wf_task_create_uid, ms.cers_wf_task_create_date, ms.cers_wf_task_completed_by_uid, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_wf_task_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge
      GROUP BY company_code,
               coid,
               cers_wf_task_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','Ref_CC_CERS_WF_Task_Merge');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge';
SET audit_type= 'RECORD_COUNT';

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from {{ params.param_parallon_ra_stage_dataset_name }}.cers_workflow_task cwt 
join {{ params.param_parallon_ra_stage_dataset_name }}.ce_workflow_profile cwp on cwt.ce_workflow_profile_id = cwp.id and cwt.schema_id = cwp.schema_id
join {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure rccos on
rccos.schema_id = cwt.schema_id
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge
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


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task AS x USING
  (SELECT ref_cc_cers_wf_task_merge.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ref_cc_cers_wf_task_merge) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cers_wf_task_id = z.cers_wf_task_id WHEN MATCHED THEN
UPDATE
SET cers_term_id = z.cers_term_id,
    ce_wf_profile_id = z.ce_wf_profile_id,
    ce_wf_profile_name = z.ce_wf_profile_name,
    cers_task_sequence = z.cers_task_sequence,
    ce_wf_task_id = z.ce_wf_task_id,
    cers_wf_task_name = z.cers_wf_task_name,
    cers_wf_task_desc = z.cers_wf_task_desc,
    cers_wf_task_expected_dur_rate = z.cers_wf_task_expected_dur_rate,
    cers_wf_task_is_atch_reqr_ind = z.cers_wf_task_is_atch_reqr_ind,
    cers_wf_task_start_date = z.cers_wf_task_start_date,
    cers_wf_task_completed_date = z.cers_wf_task_completed_date,
    cers_wf_task_is_active_ind = z.cers_wf_task_is_active_ind,
    cers_wf_task_comments = z.cers_wf_task_comments,
    cers_wf_task_is_current_ind = z.cers_wf_task_is_current_ind,
    cers_wf_task_create_uid = z.cers_wf_task_create_uid,
    cers_wf_task_create_date = z.cers_wf_task_create_date,
    cers_wf_task_completed_by_uid = z.cers_wf_task_completed_by_uid,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_wf_task_id,
        cers_term_id,
        ce_wf_profile_id,
        ce_wf_profile_name,
        cers_task_sequence,
        ce_wf_task_id,
        cers_wf_task_name,
        cers_wf_task_desc,
        cers_wf_task_expected_dur_rate,
        cers_wf_task_is_atch_reqr_ind,
        cers_wf_task_start_date,
        cers_wf_task_completed_date,
        cers_wf_task_is_active_ind,
        cers_wf_task_comments,
        cers_wf_task_is_current_ind,
        cers_wf_task_create_uid,
        cers_wf_task_create_date,
        cers_wf_task_completed_by_uid,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.cers_wf_task_id, z.cers_term_id, z.ce_wf_profile_id, z.ce_wf_profile_name, z.cers_task_sequence, z.ce_wf_task_id, z.cers_wf_task_name, z.cers_wf_task_desc, z.cers_wf_task_expected_dur_rate, z.cers_wf_task_is_atch_reqr_ind, z.cers_wf_task_start_date, z.cers_wf_task_completed_date, z.cers_wf_task_is_active_ind, z.cers_wf_task_comments, z.cers_wf_task_is_current_ind, z.cers_wf_task_create_uid, z.cers_wf_task_create_date, z.cers_wf_task_completed_by_uid, 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_wf_task_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task
      GROUP BY company_code,
               coid,
               cers_wf_task_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task
WHERE ref_cc_cers_wf_task.dw_last_update_date_time <>
    (SELECT max(ref_cc_cers_wf_task_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_wf_task AS ref_cc_cers_wf_task_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','Ref_CC_CERS_WF_Task');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;