DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_remittance_advice.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Holly Ray
          Date: 8/12/2011
          Name: CC_Remittance_Advice_Build.sql
          Mod1: Replaces Actual_Reimb_Remit table due to revised Remit Data Model.
          Mod2: Changed DB logon path for DMExpress conversion on 2/10/2012 SW.
          Mod3: Tuned SQL on 11/5/2014 SW.
          Mod4: Optimized SQL by changing case statement to coalesce and joins to ref_cc_org_structure
                on 2/26/2015 SW.
		  Mod5: Changed delete to only consider active coids on 1/30/2018 SW.
	Mod6:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod7:  -  PBI 25628  - 3/23/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
	Mod8:Added Audit Merge Expected 102220202
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA134;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg AS mt USING
  (SELECT DISTINCT temp.company_cd AS company_code,
                   substr(temp.coido, 1, 5) AS coid,
                   temp.payor_dw_id,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(temp.remittance_advice_num) AS INT64) AS remittance_advice_num,
                   temp.remittance_header_id,
                   substr(temp.unit_num, 1, 5) AS unit_num,
                   temp.ra_iplan_id AS iplan_id,
                   temp.payment_date,
                   temp.remittance_date,
                   ROUND(temp.remittance_amt, 3, 'ROUND_HALF_EVEN') AS remittance_amt,
                   substr(temp.check_num, 1, 30) AS check_num,
                   substr(temp.icn_num, 1, 15) AS icn_num,
                   substr(temp.group_control_num, 1, 15) AS group_control_num,
                   CAST(temp.create_date_time AS DATETIME) AS create_date_time,
                   temp.dw_last_update_date_time,
                   substr(temp.source_system_code, 1, 1) AS source_system_code
   FROM
     (SELECT max(reforg.company_code) AS company_cd,
             max(CASE
                     WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                     ELSE og.client_id
                 END) AS coido,
             pyro.payor_dw_id,
             substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) AS remittance_advice_num,
             ragh.id AS remittance_header_id,
             max(og.short_name) AS unit_num,
             CASE
                 WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                 ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
             END AS ra_iplan_id,
             ragh.production_date AS payment_date,
             ragh.check_date AS remittance_date,
             ragh.check_amount AS remittance_amt,
             coalesce(ragh.check_number, format('%4d', 0)) AS check_num,
             max(ragh.interchange_control_number) AS icn_num,
             max(ragh.group_control_number) AS group_control_num,
             ragh.date_created AS create_date_time,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
             'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
      AND reforg.schema_id = og.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      GROUP BY upper(reforg.company_code),
               upper(CASE
                         WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                         ELSE og.client_id
                     END),
               3,
               4,
               5,
               upper(og.short_name),
               7,
               8,
               9,
               10,
               11,
               upper(ragh.interchange_control_number),
               upper(ragh.group_control_number),
               14,
               15,
               16) AS TEMP) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.remittance_advice_num, 0) = coalesce(ms.remittance_advice_num, 0)
     AND coalesce(mt.remittance_advice_num, 1) = coalesce(ms.remittance_advice_num, 1))
AND (coalesce(mt.remittance_header_id, NUMERIC '0') = coalesce(ms.remittance_header_id, NUMERIC '0')
     AND coalesce(mt.remittance_header_id, NUMERIC '1') = coalesce(ms.remittance_header_id, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.payment_date, DATE '1970-01-01') = coalesce(ms.payment_date, DATE '1970-01-01')
     AND coalesce(mt.payment_date, DATE '1970-01-02') = coalesce(ms.payment_date, DATE '1970-01-02'))
AND (coalesce(mt.remittance_date, DATE '1970-01-01') = coalesce(ms.remittance_date, DATE '1970-01-01')
     AND coalesce(mt.remittance_date, DATE '1970-01-02') = coalesce(ms.remittance_date, DATE '1970-01-02'))
AND (coalesce(mt.remittance_amt, NUMERIC '0') = coalesce(ms.remittance_amt, NUMERIC '0')
     AND coalesce(mt.remittance_amt, NUMERIC '1') = coalesce(ms.remittance_amt, NUMERIC '1'))
AND (upper(coalesce(mt.check_num, '0')) = upper(coalesce(ms.check_num, '0'))
     AND upper(coalesce(mt.check_num, '1')) = upper(coalesce(ms.check_num, '1')))
AND (upper(coalesce(mt.icn_num, '0')) = upper(coalesce(ms.icn_num, '0'))
     AND upper(coalesce(mt.icn_num, '1')) = upper(coalesce(ms.icn_num, '1')))
AND (upper(coalesce(mt.group_control_num, '0')) = upper(coalesce(ms.group_control_num, '0'))
     AND upper(coalesce(mt.group_control_num, '1')) = upper(coalesce(ms.group_control_num, '1')))
AND (coalesce(mt.create_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.create_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.create_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.create_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        payor_dw_id,
        remittance_advice_num,
        remittance_header_id,
        unit_num,
        iplan_id,
        payment_date,
        remittance_date,
        remittance_amt,
        check_num,
        icn_num,
        group_control_num,
        create_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.payor_dw_id, ms.remittance_advice_num, ms.remittance_header_id, ms.unit_num, ms.iplan_id, ms.payment_date, ms.remittance_date, ms.remittance_amt, ms.check_num, ms.icn_num, ms.group_control_num, ms.create_date_time, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             payor_dw_id,
             remittance_advice_num,
             remittance_header_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg
      GROUP BY company_code,
               coid,
               payor_dw_id,
               remittance_advice_num,
               remittance_header_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Remittance_Advice_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
     select count(*) from (select count(*) FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
      AND reforg.schema_id = og.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN  {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
     GROUP BY upper(reforg.company_code),
               upper(CASE
                         WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                         ELSE og.client_id
                     END),
            pyro.payor_dw_id,
             substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4),
             ragh.id,
               upper(og.short_name),
            CASE
                 WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                 ELSE CAST( `{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
             END,
             ragh.production_date,
             ragh.check_date,
             ragh.check_amount,
             coalesce(ragh.check_number, format('%4d', 0)),
               upper(ragh.interchange_control_number),
               upper(ragh.group_control_number),
ragh.date_created,
             datetime_trunc(current_datetime('US/Central'), SECOND))
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg
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


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice AS x USING -- LPD

  (SELECT cc_remittance_advice_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_advice_stg) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.payor_dw_id = z.payor_dw_id
AND x.remittance_advice_num = z.remittance_advice_num
AND x.remittance_header_id = z.remittance_header_id WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    iplan_id = z.iplan_id,
    payment_date = z.payment_date,
    remittance_date = z.remittance_date,
    remittance_amt = z.remittance_amt,
    check_num = z.check_num,
    icn_num = z.icn_num,
    group_control_num = z.group_control_num,
    create_date_time = z.create_date_time,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        payor_dw_id,
        remittance_advice_num,
        remittance_header_id,
        unit_num,
        iplan_id,
        payment_date,
        remittance_date,
        remittance_amt,
        check_num,
        icn_num,
        group_control_num,
        create_date_time,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.payor_dw_id, z.remittance_advice_num, z.remittance_header_id, z.unit_num, z.iplan_id, z.payment_date, z.remittance_date, z.remittance_amt, z.check_num, z.icn_num, z.group_control_num, z.create_date_time, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             payor_dw_id,
             remittance_advice_num,
             remittance_header_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice
      GROUP BY company_code,
               coid,
               payor_dw_id,
               remittance_advice_num,
               remittance_header_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice
WHERE upper(cc_remittance_advice.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_remittance_advice.dw_last_update_date_time <>
    (SELECT max(cc_remittance_advice_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice AS cc_remittance_advice_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Remittance_Advice');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;