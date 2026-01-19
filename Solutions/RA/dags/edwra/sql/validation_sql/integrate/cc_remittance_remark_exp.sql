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
FROM (SELECT DISTINCT company_cd AS company_cd,
                   coido AS coido,
                   a.patient_dw_id AS patient_dw_id,
                   pyro.payor_dw_id AS payor_dw_id,
                   remittance_advice_num AS remittance_advice_num,
                   racp.date_created AS ra_log_date,
                   substr(CASE
                              WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                              ELSE format('%4d', 0)
                          END, 1, 4) AS log_id,
                   row_number() OVER (PARTITION BY upper(company_cd),
                                                   upper(coido),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id,
                                                   racp.date_created,
                                                   upper(pyro.log_id),
                                                   remittance_advice_num,
                                                   'MIA',
                                                   any_value(remark_code_seq)
                                      ORDER BY racp.id) AS log_sequence_num,
                                     'MIA' AS remark_code_type,
                                     any_value(remark_code_seq) AS remark_code_seq,
                                     substr(og.short_name, 1, 5) AS unit_num,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                     CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                     CASE
                                         WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                     END AS ra_iplan_id,
                                     racp.remark_code AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 0 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code IS NOT NULL
  group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date, pyro.log_id,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MIA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MIA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_mia1 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 1 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_mia1 IS NOT NULL
   group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_mia1
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MIA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MIA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_mia2 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 2 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_mia2 IS NOT NULL
      group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_mia2
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MIA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MIA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_mia3 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 3 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_mia3 IS NOT NULL
      group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_mia3
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MIA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MIA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_mia4 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 4 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_mia4 IS NOT NULL
      group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_mia4
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MOA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MOA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_moa1 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 1 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_moa1 IS NOT NULL
      group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_moa1
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MOA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MOA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_moa2 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 2 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_moa2 IS NOT NULL
   group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_moa2
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MOA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MOA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_moa3 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 3 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_moa3 IS NOT NULL
   group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_moa3
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MOA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MOA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_moa4 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 4 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_moa4 IS NOT NULL
   group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_moa4
   UNION DISTINCT SELECT DISTINCT company_cd AS company_cd,
                                  coido AS coido,
                                  a.patient_dw_id AS patient_dw_id,
                                  pyro.payor_dw_id AS payor_dw_id,
                                  remittance_advice_num AS remittance_advice_num,
                                  racp.date_created AS ra_log_date,
                                  log_id AS log_id,
                                  row_number() OVER (PARTITION BY upper(company_cd),
                                                                  upper(coido),
                                                                  a.patient_dw_id,
                                                                  pyro.payor_dw_id,
                                                                  racp.date_created,
                                                                  upper(log_id),
                                                                  remittance_advice_num,
                                                                  'MOA',
                                                                  any_value(remark_code_seq)
                                                     ORDER BY racp.id) AS log_sequence_num,
                                                    'MOA' AS remark_code_type,
                                                    any_value(remark_code_seq) AS remark_code_seq,
                                                    substr(og.short_name, 1, 5) AS unit_num,
                                                    ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                                    CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                                    CASE
                                                        WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                                        ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                                    END AS ra_iplan_id,
                                                    racp.remark_code_moa5 AS remark_code,
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
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS FLOAT64)
   CROSS JOIN UNNEST(ARRAY[ 5 ]) AS remark_code_seq
   CROSS JOIN UNNEST(ARRAY[ CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4)) AS INT64) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                                       ELSE format('%4d', 0)
                                   END, 1, 4) ]) AS log_id
   CROSS JOIN UNNEST(ARRAY[ substr(CASE
                                       WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                       ELSE og.client_id
                                   END, 1, 5) ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd
   WHERE racp.remark_code_moa5 IS NOT NULL 
   group by company_cd, coido, patient_dw_id, payor_dw_id, remittance_advice_num, ra_log_date,mpyr.payer_rank, racp.id,og.short_name, racp.account_no, pyr.code, racp.remark_code, log_id, racp.remark_code_moa5
   )
   ) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_remark
        WHERE cc_remittance_remark.dw_last_update_date_time >=
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