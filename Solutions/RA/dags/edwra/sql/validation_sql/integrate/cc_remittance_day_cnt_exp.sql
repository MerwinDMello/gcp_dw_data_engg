##########################
## Variable Declaration ##
##########################

BEGIN

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;

DECLARE expected_value, actual_value, difference NUMERIC;

DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, audit_job_name, audit_status STRING;

DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

DECLARE exp_values_list, act_values_list ARRAY<STRING>;

SET current_ts = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET srctableid = Null;

SET sourcesysnm = 'ra'; -- This needs to be added

SET srcdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_stage_dataset_name }}" , '.') as arr));

SET srctablename = concat(srcdataset_id, '.','ra_claim_payment' ); -- This needs to be added

SET tgtdataset_id = (select arr[offset(1)] from (select split("{{ params.param_parallon_ra_core_dataset_name }}" , '.') as arr));

SET tgttablename =concat(tgtdataset_id, '.', @p_targettable_name);

SET tableload_start_time = @p_tableload_start_time;

SET tableload_end_time = @p_tableload_end_time;

SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);

SET audit_job_name = @p_job_name;

SET audit_time = current_ts;

SET tolerance_percent = 0;

SET exp_values_list = -- This needs to be added
(
SELECT [format('%20d', a.row_count)]
FROM
  (
select count(*) as row_count
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
      CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) as company_cd
      where remittance_day_cnt <> 0) 
      
      ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_day_cnt
        WHERE cc_remittance_day_cnt.dw_last_update_date_time >=
   (SELECT coalesce(max(audit_control.load_end_time), date_add(timestamp_trunc(current_datetime('US/Central'), SECOND), INTERVAL -1 DAY))
	FROM {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
	WHERE upper(audit_control.job_name) = upper(audit_job_name)
	  AND audit_control.load_end_time IS NOT NULL ) 
   ) AS a -- This needs to be added
);

SET idx_length = (SELECT ARRAY_LENGTH(act_values_list));

LOOP
  SET idx = idx + 1;

  IF idx > idx_length THEN
    BREAK;
  END IF;

  SET expected_value = CAST(exp_values_list[ORDINAL(idx)] AS NUMERIC);
  SET actual_value = CAST(act_values_list[ORDINAL(idx)] AS NUMERIC);

  SET difference = 
    CASE 
    WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64)
    WHEN expected_value = 0 and actual_value = 0 Then 0
    ELSE actual_value
    END;

  SET audit_status = 
  CASE
    WHEN difference <= tolerance_percent THEN "PASS"
    ELSE "FAIL"
  END;

  IF idx = 1 THEN
    SET audit_type = "RECORD_COUNT";
  ELSE
    SET audit_type = CONCAT("INGEST_CNTRLID_",idx);
  END IF;

  --Insert statement
  INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
  VALUES
  (GENERATE_UUID(), cast(srctableid as int64), sourcesysnm, srctablename, tgttablename, audit_type,
  expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
  tableload_run_time, audit_job_name, audit_time, audit_status
   );

END LOOP;
END