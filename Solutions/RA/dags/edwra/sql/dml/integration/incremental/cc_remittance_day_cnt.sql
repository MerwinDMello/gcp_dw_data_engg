DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_remittance_day_cnt.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Holly Ray
          Date: 8/11/2011
          Name: CC_Remittance_Day_Cnt.sql
          Mod1: Replaces Actual_Reimb_Day_Cnt table due to revised Remit Data Model. - 09152011
          Mod2: Changed DB logon path for DMExpress conversion on 2/10/2012 SW.
          Mod3: Added diagnostics due to spool space errors per Teradata on 8/12/2014 SW.
          Mod4: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
		  Mod5: Changed delete to only consider active coids on 1/31/2018 SW.
		  Mod6: Removed diagnostic nohashjoin from script due to long running query on 2/20/2018 SW.
		  Mod7: Re-added diagnostic nohashjoin per DBA from script due to long running query on 3/19/2018 SW.
	Mod8:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
        Mod 9: Added Audir Merge 10202020
*************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA132;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Diagnostic noprodjoin on for session;
 -- Diagnostic nohashjoin on for session;
 -- Diagnostic noviewfold on for session;
 -- Diagnostic noparallel on for session;
 BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_day_cnt_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_day_cnt_stg AS mt USING
  (SELECT DISTINCT temp.company_cd AS company_code,
                   substr(temp.coido, 1, 5) AS coid,
                   temp.patient_dw_id,
                   temp.payor_dw_id,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(temp.remittance_advice_num) AS INT64) AS remittance_advice_num,
                   temp.ra_log_date,
                   substr(temp.log_id, 1, 4) AS log_id,
                   temp.log_sequence_num,
                   substr(temp.remittance_day_code, 1, 3) AS remittance_day_code,
                   substr(temp.unit_num, 1, 5) AS unit_num,
                   temp.ra_pat_acct_num AS pat_acct_num,
                   CAST(temp.iplan_insurance_order_num AS INT64) AS iplan_insurance_order_num,
                   temp.ra_iplan_id AS iplan_id,
                   ROUND(temp.remittance_day_cnt, 2, 'ROUND_HALF_EVEN') AS remittance_day_cnt,
                   temp.dw_last_update_date_time,
                   substr(temp.source_system_code, 1, 1) AS source_system_code
   FROM
     (SELECT company_cd AS company_cd,
             coido AS coido,
             a.patient_dw_id AS patient_dw_id,
             pyro.payor_dw_id AS payor_dw_id,
             remittance_advice_num AS remittance_advice_num,
             racp.date_created AS ra_log_date,
             CASE
                 WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                 ELSE format('%4d', 0)
             END AS log_id,
             row_number() OVER (PARTITION BY upper(company_cd),
                                             upper(coido),
                                             a.patient_dw_id,
                                             pyro.payor_dw_id,
                                             racp.date_created,
                                             upper(pyro.log_id),
                                             remittance_advice_num,
                                             'LPD'
                                ORDER BY racp.id) AS log_sequence_num,
                               'LPD' AS remittance_day_code,
                               og.short_name AS unit_num,
                               ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                               mpyr.payer_rank AS iplan_insurance_order_num,
                               CASE
                                   WHEN upper(trim(pyr.code)) = 'NO INS'
                                        OR pyr.code IS NULL THEN 0
                                   ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                               END AS ra_iplan_id,
                               racp.lifetime_psychiat_day_count AS remittance_day_cnt,
                               datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                               'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE racp.lifetime_psychiat_day_count <> 0
      UNION ALL SELECT -- CRD
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'CRD'
                    ORDER BY racp.id) AS log_sequence_num,
                   'CRD' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   racp.cost_report_day_count AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE racp.cost_report_day_count <> 0
      UNION ALL SELECT -- ACD
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ACD'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ACD' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.actual_coinsured_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0
      UNION ALL SELECT -- ALR
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ALR'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ALR' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.actual_lifetime_reserve_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0
      UNION ALL SELECT -- ELR
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ELR'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ELR' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.est_lifetime_reserve_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0
      UNION ALL SELECT -- ANC
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ANC'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ANC' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.actual_non_covered_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0
      UNION ALL SELECT -- ENC
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ENC'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ENC' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                 WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                 ELSE og.client_id
                                                                                                                             END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.est_non_covered_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0
      UNION ALL SELECT -- ODN
 company_cd AS company_cd,
 coido AS coido,
 a.patient_dw_id AS patient_dw_id,
 pyro.payor_dw_id AS payor_dw_id,
 remittance_advice_num AS remittance_advice_num,
 racp.date_created AS ra_log_date,
 CASE
     WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
     ELSE format('%4d', 0)
 END AS log_id,
 row_number() OVER (PARTITION BY upper(company_cd),
                                 upper(coido),
                                 a.patient_dw_id,
                                 pyro.payor_dw_id,
                                 racp.date_created,
                                 upper(pyro.log_id),
                                 remittance_advice_num,
                                 'ODN'
                    ORDER BY racp.id) AS log_sequence_num,
                   'ODN' AS remittance_day_code,
                   og.short_name AS unit_num,
                   ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                   mpyr.payer_rank AS iplan_insurance_order_num,
                   CASE
                       WHEN upper(trim(pyr.code)) = 'NO INS'
                            OR pyr.code IS NULL THEN 0
                       ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                   END AS ra_iplan_id,
                   remittance_day_cnt AS remittance_day_cnt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
      AND racp.schema_id = ma.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
      AND ma.schema_id = og.schema_id
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
      AND ragh.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
      AND racp.schema_id = mpyr.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
      AND pyr.schema_id = mpyr.schema_id
      INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(CASE
                                                                                                                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                ELSE og.client_id
                                                                                                                            END))
      AND reforg.schema_id = racp.schema_id
      LEFT OUTER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(CASE
                                                                                                                                      WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                                      ELSE og.client_id
                                                                                                                                  END))
      AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
      AND pyro.iplan_id = CASE
                              WHEN upper(trim(pyr.code)) = 'NO INS'
                                   OR pyr.code IS NULL THEN 0
                              ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                          END
      INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(CASE
                                                                                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                                                                                           ELSE og.client_id
                                                                                                                       END))
      AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
      AND a.pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN')
      CROSS JOIN UNNEST(ARRAY[ ROUND(racp.outlier_days, 0, 'ROUND_HALF_EVEN') ]) AS remittance_day_cnt
      CROSS JOIN UNNEST(ARRAY[ substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) ]) AS remittance_advice_num
      CROSS JOIN UNNEST(ARRAY[ CASE
                                   WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                   ELSE og.client_id
                               END ]) AS coido
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
      WHERE remittance_day_cnt <> 0 ) AS TEMP) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.remittance_advice_num, 0) = coalesce(ms.remittance_advice_num, 0)
     AND coalesce(mt.remittance_advice_num, 1) = coalesce(ms.remittance_advice_num, 1))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (upper(coalesce(mt.remittance_day_code, '0')) = upper(coalesce(ms.remittance_day_code, '0'))
     AND upper(coalesce(mt.remittance_day_code, '1')) = upper(coalesce(ms.remittance_day_code, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.remittance_day_cnt, NUMERIC '0') = coalesce(ms.remittance_day_cnt, NUMERIC '0')
     AND coalesce(mt.remittance_day_cnt, NUMERIC '1') = coalesce(ms.remittance_day_cnt, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        log_id,
        log_sequence_num,
        remittance_day_code,
        unit_num,
        pat_acct_num,
        iplan_insurance_order_num,
        iplan_id,
        remittance_day_cnt,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.remittance_advice_num, ms.ra_log_date, ms.log_id, ms.log_sequence_num, ms.remittance_day_code, ms.unit_num, ms.pat_acct_num, ms.iplan_insurance_order_num, ms.iplan_id, ms.remittance_day_cnt, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             remittance_advice_num,
             ra_log_date,
             log_id,
             log_sequence_num,
             remittance_day_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_day_cnt_stg
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               remittance_advice_num,
               ra_log_date,
               log_id,
               log_sequence_num,
               remittance_day_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_day_cnt_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_Remittance_Day_Cnt_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt AS x USING -- LPD

  (SELECT cc_remittance_day_cnt_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_day_cnt_stg) AS z ON x.patient_dw_id = z.patient_dw_id
AND upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.payor_dw_id = z.payor_dw_id
AND x.remittance_advice_num = z.remittance_advice_num
AND x.ra_log_date = z.ra_log_date
AND upper(rtrim(x.log_id)) = upper(rtrim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND upper(rtrim(x.remittance_day_code)) = upper(rtrim(z.remittance_day_code)) WHEN MATCHED THEN
UPDATE
SET remittance_day_code = z.remittance_day_code,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_insurance_order_num = z.iplan_insurance_order_num,
    iplan_id = z.iplan_id,
    remittance_day_cnt = z.remittance_day_cnt,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        log_id,
        log_sequence_num,
        remittance_day_code,
        unit_num,
        pat_acct_num,
        iplan_insurance_order_num,
        iplan_id,
        remittance_day_cnt,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.remittance_advice_num, z.ra_log_date, z.log_id, z.log_sequence_num, z.remittance_day_code, z.unit_num, z.pat_acct_num, z.iplan_insurance_order_num, z.iplan_id, z.remittance_day_cnt, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             patient_dw_id,
             payor_dw_id,
             remittance_advice_num,
             ra_log_date,
             log_id,
             log_sequence_num,
             remittance_day_code
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               remittance_advice_num,
               ra_log_date,
               log_id,
               log_sequence_num,
               remittance_day_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt
WHERE upper(cc_remittance_day_cnt.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_remittance_day_cnt.dw_last_update_date_time <>
    (SELECT max(cc_remittance_day_cnt_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt AS cc_remittance_day_cnt_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Remittance_Day_Cnt');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;