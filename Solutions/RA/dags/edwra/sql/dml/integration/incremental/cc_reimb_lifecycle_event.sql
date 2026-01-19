DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_reimb_lifecycle_event.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************
   Developer: Holly Ray
        Date: 10/25/2011
        Name: CC_Reimb_Lifecycle_Event.sql
        Mod1: Based on revised EOR model.
	Mod2: Removed CAST on Patient Account number on 1/14/2015 AS.
	Mod3: Optimized SQL on 2/4/2015 SW.
	Mod4: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
	Mod5: Changed delete to only consider active coids on 1/31/2018 SW.
	Mod6:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod7:Added Audit Merge 07062021
************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA187;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nohashjoin on for session;
 BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimb_lifecycle_event_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimb_lifecycle_event_merge AS mt USING
  (SELECT DISTINCT company_cd AS company_cd,
                   trim(coido) AS coido,
                   a.patient_dw_id,
                   pyro.payor_dw_id AS payor_dw_id,
                   CAST(mapcl.payer_rank AS INT64) AS iplan_insurance_order_num,
                   DATE(mapcl.calculation_date) AS eor_log_date,
                   substr(concat('INS', trim(format('%#4.0f', mapcl.payer_rank))), 1, 4) AS log_id,
                   row_number() OVER (PARTITION BY upper(company_cd),
                                                   trim(upper(coido)),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id
                                      ORDER BY mapl.id) AS log_sequence_num,
                                     DATE(mapcl.calculation_date) AS eff_from_date,
                                     mapl.id AS reimb_lifecycle_id,
                                     reforg.unit_num AS unit_num,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_nbr,
                                     CASE
                                         WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                                     END AS life_iplan_id,
                                     mapl.lifecycle_date,
                                     ROUND(mapl.expected_payment, 3, 'ROUND_HALF_EVEN') AS expected_payment_amt,
                                     ROUND(mapl.actual_payment, 3, 'ROUND_HALF_EVEN') AS actual_payment_amt,
                                     ROUND(mapl.payer_amount_due, 3, 'ROUND_HALF_EVEN') AS payor_due_amt,
                                     mapl.lifecycle_event AS lifecycle_event_type_id,
                                     mapl.mon_status_id AS account_payer_status_id,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                     'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_lifecycle AS mapl
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON mapl.mon_account_payer_id = mapyr.id
   AND mapl.schema_id = mapyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON mapyr.mon_account_id = ma.id
   AND mapyr.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcl.mon_account_payer_id = mapl.mon_account_payer_id
   AND mapcl.schema_id = mapl.schema_id
   AND mapcl.service_date_begin < current_date('US/Central')
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mapyr.mon_payer_id = mpyr.id
   AND mapyr.schema_id = mpyr.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(reforg.coid)) = upper(rtrim(a.coid))
   AND upper(rtrim(reforg.company_code)) = upper(rtrim(a.company_code))
   AND ma.account_no = a.pat_acct_num
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   CROSS JOIN UNNEST(ARRAY[ reforg.coid ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_cd, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_cd, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coido, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.eor_log_date, DATE '1970-01-01') = coalesce(ms.eor_log_date, DATE '1970-01-01')
     AND coalesce(mt.eor_log_date, DATE '1970-01-02') = coalesce(ms.eor_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.eff_from_date, DATE '1970-01-01') = coalesce(ms.eff_from_date, DATE '1970-01-01')
     AND coalesce(mt.eff_from_date, DATE '1970-01-02') = coalesce(ms.eff_from_date, DATE '1970-01-02'))
AND (coalesce(mt.reimb_lifecycle_id, NUMERIC '0') = coalesce(ms.reimb_lifecycle_id, NUMERIC '0')
     AND coalesce(mt.reimb_lifecycle_id, NUMERIC '1') = coalesce(ms.reimb_lifecycle_id, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_nbr, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_nbr, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.life_iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.life_iplan_id, 1))
AND (coalesce(mt.lifecycle_date, DATE '1970-01-01') = coalesce(ms.lifecycle_date, DATE '1970-01-01')
     AND coalesce(mt.lifecycle_date, DATE '1970-01-02') = coalesce(ms.lifecycle_date, DATE '1970-01-02'))
AND (coalesce(mt.expected_payment_amt, NUMERIC '0') = coalesce(ms.expected_payment_amt, NUMERIC '0')
     AND coalesce(mt.expected_payment_amt, NUMERIC '1') = coalesce(ms.expected_payment_amt, NUMERIC '1'))
AND (coalesce(mt.actual_payment_amt, NUMERIC '0') = coalesce(ms.actual_payment_amt, NUMERIC '0')
     AND coalesce(mt.actual_payment_amt, NUMERIC '1') = coalesce(ms.actual_payment_amt, NUMERIC '1'))
AND (coalesce(mt.payor_due_amt, NUMERIC '0') = coalesce(ms.payor_due_amt, NUMERIC '0')
     AND coalesce(mt.payor_due_amt, NUMERIC '1') = coalesce(ms.payor_due_amt, NUMERIC '1'))
AND (coalesce(mt.lifecycle_event_type_id, NUMERIC '0') = coalesce(ms.lifecycle_event_type_id, NUMERIC '0')
     AND coalesce(mt.lifecycle_event_type_id, NUMERIC '1') = coalesce(ms.lifecycle_event_type_id, NUMERIC '1'))
AND (coalesce(mt.account_payer_status_id, NUMERIC '0') = coalesce(ms.account_payer_status_id, NUMERIC '0')
     AND coalesce(mt.account_payer_status_id, NUMERIC '1') = coalesce(ms.account_payer_status_id, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        reimb_lifecycle_id,
        unit_num,
        pat_acct_num,
        iplan_id,
        lifecycle_date,
        expected_payment_amt,
        actual_payment_amt,
        payor_due_amt,
        lifecycle_event_type_id,
        account_payer_status_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_cd, ms.coido, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.reimb_lifecycle_id, ms.unit_num, ms.pat_acct_nbr, ms.life_iplan_id, ms.lifecycle_date, ms.expected_payment_amt, ms.actual_payment_amt, ms.payor_due_amt, ms.lifecycle_event_type_id, ms.account_payer_status_id, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             log_id,
             log_sequence_num,
             eff_from_date,
             reimb_lifecycle_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimb_lifecycle_event_merge
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date,
               reimb_lifecycle_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.cc_reimb_lifecycle_event_merge');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Reimb_Lifecycle_Event_Merge');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = 'edwra_staging.mon_account';
SET tgttablename = 'edwra_staging.cc_reimb_lifecycle_event_merge';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) from edwra_staging.mon_account_payer_lifecycle mapl
join edwra_staging.mon_account_payer mapyr on
     mapl.mon_account_payer_id = mapyr.id and
     mapl.schema_id = mapyr.schema_id
join edwra_staging.mon_account ma on 
     mapyr.mon_account_id = ma.id and
     mapyr.schema_id  = ma.schema_id
join edwra_staging.org  og on
     ma.org_id_provider = og.org_id and
     ma.schema_id = og.schema_id
join edwra.ref_cc_org_structure reforg on 
     reforg.coid = substr(og.client_id,7,5) and
     reforg.schema_id = og.schema_id
join edwra_staging.mon_account_payer_calc_latest mapcl on
     mapcl.mon_account_payer_id = mapl.mon_account_payer_id and
     mapcl.schema_id = mapl.schema_id and
     mapcl.service_date_begin < current_date
join edwra_staging.mon_payer mpyr on
     mapyr.mon_payer_id = mpyr.id and
     mapyr.schema_id = mpyr.schema_id                               
join edwra_staging.clinical_acctkeys a  on 
     reforg.coid = a.coid and
     reforg.company_code = a.company_code and
     ma.account_no = a.pat_acct_num
join {{ params.param_auth_base_views_dataset_name }}.payor_organization pyro on
     pyro.coid = reforg.coid and 
     pyro.company_code = reforg.company_code and 
     pyro.iplan_id = case when trim(mpyr.code) = 'no ins' then 0 else cast(substr(trim(mpyr.code),1,3)||substr(trim(mpyr.code),5,2) as integer) end

);

SET actual_value =
(
select count(*) as row_count
from edwra_staging.cc_reimb_lifecycle_event_merge
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

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event AS x USING
  (SELECT cc_reimb_lifecycle_event_merge.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_reimb_lifecycle_event_merge) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_insurance_order_num = z.iplan_insurance_order_num
AND x.eor_log_date = z.eor_log_date
AND upper(rtrim(x.log_id)) = upper(rtrim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND x.reimb_lifecycle_id = z.reimb_lifecycle_id
AND x.eff_from_date = z.eff_from_date WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    lifecycle_date = z.lifecycle_date,
    expected_payment_amt = z.expected_payment_amt,
    actual_payment_amt = z.actual_payment_amt,
    payor_due_amt = z.payor_due_amt,
    lifecycle_event_type_id = z.lifecycle_event_type_id,
    account_payer_status_id = z.account_payer_status_id,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        iplan_insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        reimb_lifecycle_id,
        unit_num,
        pat_acct_num,
        iplan_id,
        lifecycle_date,
        expected_payment_amt,
        actual_payment_amt,
        payor_due_amt,
        lifecycle_event_type_id,
        account_payer_status_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.iplan_insurance_order_num, z.eor_log_date, z.log_id, z.log_sequence_num, z.eff_from_date, z.reimb_lifecycle_id, z.unit_num, z.pat_acct_num, z.iplan_id, z.lifecycle_date, z.expected_payment_amt, z.actual_payment_amt, z.payor_due_amt, z.lifecycle_event_type_id, z.account_payer_status_id, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             iplan_insurance_order_num,
             eor_log_date,
             log_id,
             log_sequence_num,
             eff_from_date,
             reimb_lifecycle_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date,
               reimb_lifecycle_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.cc_reimb_lifecycle_event');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event
WHERE trim(upper(cc_reimb_lifecycle_event.coid)) IN -- adding trim to catch historical records
    (SELECT trim(upper(r.coid)) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_reimb_lifecycle_event.dw_last_update_date_time <>
    (SELECT max(cc_reimb_lifecycle_event_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_reimb_lifecycle_event AS cc_reimb_lifecycle_event_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Reimb_Lifecycle_Event');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;