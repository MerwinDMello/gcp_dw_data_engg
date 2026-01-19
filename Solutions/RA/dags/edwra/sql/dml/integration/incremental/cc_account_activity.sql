DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_account_activity.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************************************************
     Developer: Holly Ray
          Date: 8/13/2011
          Name: CC_Account_Activity.sql
          Mod1: Replaces original CC_Account_Activity table due to revised Activity Data Model.
          Mod2: Added LOJ to Users for getting the login id to changes made to the target table
                as this was a defect on 12/6/2011 SW.
          Mod3: Added Diagnostics per Teradata for long running queries on 6/30/2014 FY.
          Mod4: Added additional diagnostics per Teradata on 8/12/2014 SW.
          Mod5: Tuned SQL for performance, removed diagnostics on 11/17/2014 SW.
          Mod6: Removed CAST on Patient Account Number on 1/13/2014. AS
          Mod7: Optimized SQL by using Ref_CC_Org_Structure to get clinical_acctkeys on 2/25/2015 SW.
          Mod8: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
		  Mod9: Modified join to Mon_Payer from Mon_Account_Payer to Mon_Account_Activity instead.  5/16/2016  JC
		  Mod10:Modified join to EDWPF_STAGING.Payor_Organization to use a view, EDWPF_VIEWS.Payor_Organization on
		        11/1/2017. SW.
	 Mod11: Optimized script to add temp table  6/12/2018  PT
	Mod12:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod13: Audit Merge PBI 25190  - Saravana Moorthy
**********************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA216;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_activity_merge_stage;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_activity_merge_stage AS mt USING
  (SELECT DISTINCT a.patient_dw_id AS patient_dw_id,
                   maa.id AS activity_id,
                   reforg.company_code AS company_cd,
                   reforg.coid AS coid,
                   og.short_name AS unit_num,
                   pyro.payor_dw_id AS payor_dw_id,
                   ROUND(CASE
                             WHEN mpyr.payer_rank IS NULL THEN CAST(0 AS NUMERIC)
                             ELSE mpyr.payer_rank
                         END, 0, 'ROUND_HALF_EVEN') AS iplan_insurance_order_num,
                   substr(format('%#14.0f', ma.account_no), 1, 38) AS act_pat_acct_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS act_iplan_id,
                   maa.created_datetime AS activity_create_date_time,
                   maa.date_updated AS activity_update_date_time,
                   usr.login_id AS create_login_userid,
                   maa.subject AS activity_subject_text,
                   maa.description AS activity_desc,
                   maa.expected_duration AS expected_duration_num,
                   maa.due_date AS activity_due_date,
                   ousr.login_id AS activity_owner_login_userid,
                   maa.did_it_create_appeal_or_coll AS create_appeal_or_coll_ind,
                   maa.activity_type_id AS activity_type_id,
                   maa.status_id AS activity_status_id,
                   maa.completion_datetime AS activity_complete_date_time,
                   cusr.login_id AS complete_login_userid,
                   maa.resolution AS activity_resolve_text,
                   substr(CASE
                              WHEN maa.batch_or_thread_id <> 0 THEN 'W'
                              WHEN maa.activity_type_id <= 10000 THEN 'S'
                              ELSE 'A'
                          END, 1, 2) AS activity_source_code,
                   substr(CASE maa.is_deleted
                              WHEN 1 THEN 'N'
                              WHEN 0 THEN 'Y'
                          END, 1, 2) AS active_ind
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity AS maa
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON maa.mon_account_id = ma.id
   AND maa.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = og.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON maa.mon_account_payer_id = mpyr.id
   AND maa.schema_id = mpyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON maa.mon_payer_id = pyr.id
   AND maa.schema_id = pyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS usr ON maa.user_id_creator = usr.user_id
   AND maa.schema_id = usr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS ousr ON maa.user_id_activity_owner = ousr.user_id
   AND maa.schema_id = ousr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.users AS cusr ON maa.user_id_completed_by = cusr.user_id
   AND maa.schema_id = cusr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS'
                                OR pyr.code IS NULL THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN -- MSK
 {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.activity_id, NUMERIC '0') = coalesce(ms.activity_id, NUMERIC '0')
     AND coalesce(mt.activity_id, NUMERIC '1') = coalesce(ms.activity_id, NUMERIC '1'))
AND (upper(coalesce(mt.company_cd, '0')) = upper(coalesce(ms.company_cd, '0'))
     AND upper(coalesce(mt.company_cd, '1')) = upper(coalesce(ms.company_cd, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, NUMERIC '0') = coalesce(ms.iplan_insurance_order_num, NUMERIC '0')
     AND coalesce(mt.iplan_insurance_order_num, NUMERIC '1') = coalesce(ms.iplan_insurance_order_num, NUMERIC '1'))
AND (upper(coalesce(mt.act_pat_acct_num, '0')) = upper(coalesce(ms.act_pat_acct_num, '0'))
     AND upper(coalesce(mt.act_pat_acct_num, '1')) = upper(coalesce(ms.act_pat_acct_num, '1')))
AND (coalesce(mt.act_iplan_id, 0) = coalesce(ms.act_iplan_id, 0)
     AND coalesce(mt.act_iplan_id, 1) = coalesce(ms.act_iplan_id, 1))
AND (coalesce(mt.activity_create_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.activity_create_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.activity_create_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.activity_create_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.activity_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.activity_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.activity_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.activity_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.create_login_userid, '0')) = upper(coalesce(ms.create_login_userid, '0'))
     AND upper(coalesce(mt.create_login_userid, '1')) = upper(coalesce(ms.create_login_userid, '1')))
AND (upper(coalesce(mt.activity_subject_text, '0')) = upper(coalesce(ms.activity_subject_text, '0'))
     AND upper(coalesce(mt.activity_subject_text, '1')) = upper(coalesce(ms.activity_subject_text, '1')))
AND (upper(coalesce(mt.activity_desc, '0')) = upper(coalesce(ms.activity_desc, '0'))
     AND upper(coalesce(mt.activity_desc, '1')) = upper(coalesce(ms.activity_desc, '1')))
AND (coalesce(mt.expected_duration_num, 0) = coalesce(ms.expected_duration_num, 0)
     AND coalesce(mt.expected_duration_num, 1) = coalesce(ms.expected_duration_num, 1))
AND (coalesce(mt.activity_due_date, DATE '1970-01-01') = coalesce(ms.activity_due_date, DATE '1970-01-01')
     AND coalesce(mt.activity_due_date, DATE '1970-01-02') = coalesce(ms.activity_due_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.activity_owner_login_userid, '0')) = upper(coalesce(ms.activity_owner_login_userid, '0'))
     AND upper(coalesce(mt.activity_owner_login_userid, '1')) = upper(coalesce(ms.activity_owner_login_userid, '1')))
AND (upper(coalesce(mt.create_appeal_or_coll_ind, '0')) = upper(coalesce(ms.create_appeal_or_coll_ind, '0'))
     AND upper(coalesce(mt.create_appeal_or_coll_ind, '1')) = upper(coalesce(ms.create_appeal_or_coll_ind, '1')))
AND (coalesce(mt.activity_type_id, NUMERIC '0') = coalesce(ms.activity_type_id, NUMERIC '0')
     AND coalesce(mt.activity_type_id, NUMERIC '1') = coalesce(ms.activity_type_id, NUMERIC '1'))
AND (coalesce(mt.activity_status_id, NUMERIC '0') = coalesce(ms.activity_status_id, NUMERIC '0')
     AND coalesce(mt.activity_status_id, NUMERIC '1') = coalesce(ms.activity_status_id, NUMERIC '1'))
AND (coalesce(mt.activity_complete_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.activity_complete_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.activity_complete_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.activity_complete_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.complete_login_userid, '0')) = upper(coalesce(ms.complete_login_userid, '0'))
     AND upper(coalesce(mt.complete_login_userid, '1')) = upper(coalesce(ms.complete_login_userid, '1')))
AND (upper(coalesce(mt.activity_resolve_text, '0')) = upper(coalesce(ms.activity_resolve_text, '0'))
     AND upper(coalesce(mt.activity_resolve_text, '1')) = upper(coalesce(ms.activity_resolve_text, '1')))
AND (upper(coalesce(mt.activity_source_code, '0')) = upper(coalesce(ms.activity_source_code, '0'))
     AND upper(coalesce(mt.activity_source_code, '1')) = upper(coalesce(ms.activity_source_code, '1')))
AND (upper(coalesce(mt.active_ind, '0')) = upper(coalesce(ms.active_ind, '0'))
     AND upper(coalesce(mt.active_ind, '1')) = upper(coalesce(ms.active_ind, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        activity_id,
        company_cd,
        coid,
        unit_num,
        payor_dw_id,
        iplan_insurance_order_num,
        act_pat_acct_num,
        act_iplan_id,
        activity_create_date_time,
        activity_update_date_time,
        create_login_userid,
        activity_subject_text,
        activity_desc,
        expected_duration_num,
        activity_due_date,
        activity_owner_login_userid,
        create_appeal_or_coll_ind,
        activity_type_id,
        activity_status_id,
        activity_complete_date_time,
        complete_login_userid,
        activity_resolve_text,
        activity_source_code,
        active_ind)
VALUES (ms.patient_dw_id, ms.activity_id, ms.company_cd, ms.coid, ms.unit_num, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.act_pat_acct_num, ms.act_iplan_id, ms.activity_create_date_time, ms.activity_update_date_time, ms.create_login_userid, ms.activity_subject_text, ms.activity_desc, ms.expected_duration_num, ms.activity_due_date, ms.activity_owner_login_userid, ms.create_appeal_or_coll_ind, ms.activity_type_id, ms.activity_status_id, ms.activity_complete_date_time, ms.complete_login_userid, ms.activity_resolve_text, ms.activity_source_code, ms.active_ind);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Account_Activity_Merge_Stage');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  Audit Addition --MSK PBI 25190
BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity';
SET tgttablename = '{{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity maa
join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account ma on
     maa.mon_account_id = ma.id and
     maa.schema_id = ma.schema_id
join edwra_edwra_stagingra_staging.org og on
     ma.org_id_provider = og.org_id and
     ma.schema_id = og.schema_id
join {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure reforg on
     reforg.coid = substr(og.client_id,7,5) and
     reforg.schema_id = og.schema_id
left outer join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer mpyr on
     maa.mon_account_payer_id = mpyr.id and
     maa.schema_id = mpyr.schema_id   
left outer join {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer pyr on
     maa.mon_payer_id = pyr.id and
     maa.schema_id = pyr.schema_id
left outer join {{ params.param_parallon_ra_stage_dataset_name }}.users usr on
     maa.user_id_creator = usr.user_id and
     maa.schema_id = usr.schema_id
left outer join {{ params.param_parallon_ra_stage_dataset_name }}.users ousr on
     maa.user_id_activity_owner = ousr.user_id and
     maa.schema_id = ousr.schema_id
left outer join {{ params.param_parallon_ra_stage_dataset_name }}.users cusr on
     maa.user_id_completed_by = cusr.user_id and
     maa.schema_id = cusr.schema_id                                 
left outer join  {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan  pyro on  --msk 
     pyro.coid = reforg.coid and  
     pyro.company_code = reforg.company_code and 
     pyro.iplan_id = case when trim(pyr.code) = 'no ins' or pyr.code is null then 0 else cast(substr(trim(pyr.code),1,3)||substr(trim(pyr.code),5,2) as integer) end  
join {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys a on
     a.coid = reforg.coid and
     a.company_code = reforg.company_code and
     a.pat_acct_num = ma.account_no
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity
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

-- MSK PBI 25190
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity AS x USING
  (SELECT cc_account_activity_merge_stage.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_activity_merge_stage) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.activity_id = z.activity_id WHEN MATCHED THEN
UPDATE
SET company_code = z.company_cd,
    coid = z.coid,
    unit_num = substr(z.unit_num, 1, 5),
    payor_dw_id = z.payor_dw_id,
    iplan_insurance_order_num = CAST(z.iplan_insurance_order_num AS INT64),
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.act_pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    iplan_id = z.act_iplan_id,
    activity_create_date_time = z.activity_create_date_time,
    activity_update_date_time = z.activity_update_date_time,
    create_login_userid = substr(z.create_login_userid, 1, 20),
    activity_subject_text = z.activity_subject_text,
    activity_desc = substr(z.activity_desc, 1, 200),
    expected_duration_num = z.expected_duration_num,
    activity_due_date = z.activity_due_date,
    activity_owner_login_userid = substr(z.activity_owner_login_userid, 1, 20),
    create_appeal_or_coll_ind = z.create_appeal_or_coll_ind,
    activity_type_id = CAST(ROUND(z.activity_type_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC),
    activity_status_id = z.activity_status_id,
    activity_complete_date_time = z.activity_complete_date_time,
    complete_login_userid = substr(z.complete_login_userid, 1, 20),
    activity_resolve_text = substr(z.activity_resolve_text, 1, 200),
    activity_source_code = substr(z.activity_source_code, 1, 1),
    active_ind = substr(z.active_ind, 1, 1),
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        activity_id,
        company_code,
        coid,
        unit_num,
        payor_dw_id,
        iplan_insurance_order_num,
        pat_acct_num,
        iplan_id,
        activity_create_date_time,
        activity_update_date_time,
        create_login_userid,
        activity_subject_text,
        activity_desc,
        expected_duration_num,
        activity_due_date,
        activity_owner_login_userid,
        create_appeal_or_coll_ind,
        activity_type_id,
        activity_status_id,
        activity_complete_date_time,
        complete_login_userid,
        activity_resolve_text,
        activity_source_code,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.activity_id, z.company_cd, z.coid, substr(z.unit_num, 1, 5), z.payor_dw_id, CAST(z.iplan_insurance_order_num AS INT64), ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.act_pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), z.act_iplan_id, z.activity_create_date_time, z.activity_update_date_time, substr(z.create_login_userid, 1, 20), z.activity_subject_text, substr(z.activity_desc, 1, 200), z.expected_duration_num, z.activity_due_date, substr(z.activity_owner_login_userid, 1, 20), z.create_appeal_or_coll_ind, CAST(ROUND(z.activity_type_id, 0, 'ROUND_HALF_EVEN') AS NUMERIC), z.activity_status_id, z.activity_complete_date_time, substr(z.complete_login_userid, 1, 20), substr(z.activity_resolve_text, 1, 200), substr(z.activity_source_code, 1, 1), substr(z.active_ind, 1, 1), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             activity_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity
      GROUP BY patient_dw_id,
               activity_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_activity');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('edwra','CC_Account_Activity');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_activity_merge_stage;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;