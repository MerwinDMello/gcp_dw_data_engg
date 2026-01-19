DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_remit_amt_stg.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Ashan
       Date: 03/15/2014
       Name: EOR_Remit_Amt_Stg.sql
       Mod1: Created Staging table for the first time. 03/17/2014 AP.
       Mod2: Removed CAST on Patient Account number on 1/14/2015 AS.
       Mod3: Added diagnostics to speed up query on 2/9/2015 SW.
       Mod4: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
       Mod4: Corrected (Racp.Id As Decimal (18, 0)) to be (Racp.Id As Decimal (32, 0))
             in support of column expansion project. Numeric overflow errors
             were occurring before the change 3/9/2017 SW.
****************************************************************************/ -- DIAGNOSTIC noprodjoin ON FOR SESSION;
 -- DIAGNOSTIC nohashjoin ON FOR SESSION;
 -- DIAGNOSTIC noviewfold ON FOR SESSION;
 -- DIAGNOSTIC noparallel ON FOR SESSION;
 BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg AS mt USING
  (SELECT DISTINCT max(reforg.company_code) AS company_code,
                   max(CASE
                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                           ELSE og.client_id
                       END) AS coid,
                   a.patient_dw_id,
                   pyro.payor_dw_id,
                   substr(max(substr(trim(format('%#34.0f', racp.ra_group_header_id)), length(trim(format('%#34.0f', racp.ra_group_header_id))) - 4, 4)), 1, 4) AS remittance_advice_num,
                   racp.date_created AS ra_log_date,
                   racp.id AS rcpid,
                   substr(max(concat('INS', trim(format('%11d', CAST(mpyr.payer_rank AS INT64))))), 1, 14) AS log_id,
                   row_number() OVER (PARTITION BY max(reforg.company_code),
                                                   max(CASE
                                                           WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                                           ELSE og.client_id
                                                       END),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id,
                                                   upper(substr(max(substr(trim(format('%#34.0f', racp.ra_group_header_id)), length(trim(format('%#34.0f', racp.ra_group_header_id))) - 4, 4)), 1, 4)),
                                                   racp.date_created,
                                                   upper(substr(max(concat('INS', trim(format('%11d', CAST(mpyr.payer_rank AS INT64))))), 1, 14))
                                      ORDER BY racp.id) AS log_sequence_num,
                                     max(og.short_name) AS unit_num,
                                     ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS ra_pat_acct_num,
                                     CAST(mpyr.payer_rank AS INT64) AS iplan_insurance_order_num,
                                     CASE
                                         WHEN upper(trim(pyr.code)) = 'NO INS'
                                              OR pyr.code IS NULL THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS int64)
                                     END AS ra_iplan_id,
                                     substr(max(mpyr.rate_schedule_name), 1, 150) AS rate_schedule_name,
                                     substr(max(mpyr.misc_char01), 1, 100) AS financial_class,
                                     substr(max(mpt.ip_op_ind), 1, 1) AS ip_op_ind,
                                     sum(coalesce(racp.claim_ppscapital_amount, CAST(0 AS NUMERIC))) AS claim_ppscapital_amount,
                                     sum(coalesce(racp.claim_pps_capital_outlier_amnt, CAST(0 AS NUMERIC))) AS claim_pps_capital_outlier_amnt,
                                     sum(coalesce(racp.pps_operat_outlier_amount, CAST(0 AS NUMERIC))) AS pps_operat_outlier_amount,
                                     sum(coalesce(racp.claim_hcpcs_payable_amount, CAST(0 AS NUMERIC))) AS claim_hcpcs_payable_amount,
                                     sum(coalesce(racp.claim_payment_amount, CAST(0 AS NUMERIC))) AS claim_payment_amount,
                                     sum(coalesce(racp.claim_drg_amount, CAST(0 AS NUMERIC))) AS claim_drg_amount,
                                     sum(coalesce(ra835agg.ra835_cat_110, CAST(0 AS BIGNUMERIC))) AS ra835_cat_110,
                                     sum(coalesce(ra835agg.ra835_cat_510, CAST(0 AS BIGNUMERIC))) AS ra835_cat_510,
                                     sum(coalesce(ra835agg.ra835_cat_520, CAST(0 AS BIGNUMERIC))) AS ra835_cat_520,
                                     sum(coalesce(ra835agg.ra835_cat_550, CAST(0 AS BIGNUMERIC))) AS ra835_cat_550,
                                     sum(coalesce(ra835agg.ra835_cat_301_399, CAST(0 AS BIGNUMERIC))) AS ra835_cat_301_399,
                                     sum(coalesce(ra835agg.ra835_cat_501_599, CAST(0 AS BIGNUMERIC))) AS ra835_cat_501_599,
                                     sum(coalesce(rasrv.rasrvadj_pr_code, CAST(0 AS BIGNUMERIC))) AS rasrvadj_pr_code,
                                     sum(coalesce(rasrv.rasrvadj_oa_code, CAST(0 AS BIGNUMERIC))) AS rasrvadj_oa_code,
                                     sum(coalesce(raclaimadj.raclaimadj_pr_code, CAST(0 AS BIGNUMERIC))) AS raclaimadj_pr_code,
                                     sum(coalesce(raclaimadj.raclaimadj_oa_code, CAST(0 AS BIGNUMERIC))) AS raclaimadj_oa_code,
                                     sum(coalesce(raclaimsup.raclaimsup_amt_i_t, CAST(0 AS BIGNUMERIC))) AS raclaimsup_amt_i_t,
                                     sum(coalesce(raclaimsup.raclaimsup_amt_zm, CAST(0 AS BIGNUMERIC))) AS raclaimsup_amt_zm,
                                     sum(coalesce(rasrv.rasrv_charge_amt_cat46, CAST(0 AS BIGNUMERIC))) AS rasrv_charge_amt_cat46,
                                     sum(coalesce(rasrv.rasrv_charge_amt_cat49, CAST(0 AS BIGNUMERIC))) AS rasrv_charge_amt_cat49,
                                     sum(coalesce(rasrv.rasrv_prov_pymnt_amt_cat50, CAST(0 AS BIGNUMERIC))) AS rasrv_prov_pymnt_amt_cat50,
                                     sum(coalesce(rasrv.rasrv_prov_pymnt_amt_cat88, CAST(0 AS BIGNUMERIC))) AS rasrv_prov_pymnt_amt_cat88,
                                     sum(coalesce(rasrv.rasrv_prov_pymnt_amt_cat93, CAST(0 AS BIGNUMERIC))) AS rasrv_prov_pymnt_amt_cat93,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON racp.org_id = og.org_id
   AND racp.schema_id = og.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
   AND racp.schema_id = mpyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
   AND pyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON ma.id = racp.mon_account_id
   AND ma.schema_id = racp.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON mpt.id = ma.mon_patient_type_id
   AND mpt.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = racp.schema_id
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.payor_organization AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS'
                                OR pyr.code IS NULL THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS int64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(CAST(ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS STRING)) AS FLOAT64) --CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(racp.account_no) AS float64), 0, 'ROUND_HALF_EVEN')) AS FLOAT64)
   LEFT OUTER JOIN
     (SELECT ra835.ra_claim_payment_id,
             ra835.schema_id,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id = 110 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_110,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id = 510 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_510,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id = 520 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_520,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id = 550 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_550,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id BETWEEN 301 AND 399 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_301_399,
             sum(coalesce(CASE
                              WHEN ra835.ra_category_id BETWEEN 501 AND 599 THEN ra835.amount
                          END, CAST(0 AS NUMERIC))) AS ra835_cat_501_599
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated AS ra835
      WHERE ra835.ra_category_id BETWEEN 301 AND 399
        OR ra835.ra_category_id BETWEEN 501 AND 599
        OR ra835.ra_category_id = 110
      GROUP BY 1,
               2) AS ra835agg ON ra835agg.ra_claim_payment_id = racp.id
   AND ra835agg.schema_id = racp.schema_id
   LEFT OUTER JOIN
     (SELECT ca.ra_claim_payment_id,
             ca.schema_id,
             sum(coalesce(CASE
                              WHEN upper(rtrim(ca.group_code)) = 'PR'
                                   AND upper(rtrim(ca.reasn_code)) = '66' THEN ca.amount
                          END, CAST(0 AS NUMERIC))) AS raclaimadj_pr_code,
             sum(coalesce(CASE
                              WHEN upper(rtrim(ca.group_code)) = 'OA'
                                   AND upper(rtrim(ca.reasn_code)) IN('23', '71') THEN ca.amount
                          END, CAST(0 AS NUMERIC))) AS raclaimadj_oa_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_adjustment AS ca
      WHERE upper(rtrim(ca.group_code)) IN('PR',
                                           'OA')
        AND upper(rtrim(ca.reasn_code)) IN('66',
                                           '23',
                                           '71')
      GROUP BY 1,
               2) AS raclaimadj ON raclaimadj.ra_claim_payment_id = racp.id
   AND raclaimadj.schema_id = racp.schema_id
   LEFT OUTER JOIN
     (SELECT racs.ra_claim_payment_id,
             racs.schema_id,
             sum(coalesce(CASE
                              WHEN upper(rtrim(racs.supplement_type)) IN('I', 'T') THEN racs.supplement_amount
                          END, CAST(0 AS NUMERIC))) AS raclaimsup_amt_i_t,
             sum(coalesce(CASE
                              WHEN upper(rtrim(racs.supplement_type)) = 'ZM' THEN racs.supplement_amount
                          END, CAST(0 AS NUMERIC))) AS raclaimsup_amt_zm
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_supplement AS racs
      WHERE upper(rtrim(racs.supplement_type)) IN('I',
                                                  'T',
                                                  'ZM')
      GROUP BY 1,
               2) AS raclaimsup ON raclaimsup.ra_claim_payment_id = racp.id
   AND raclaimsup.schema_id = racp.schema_id
   LEFT OUTER JOIN
     (SELECT s.ra_claim_payment_id,
             s.schema_id,
             sum(coalesce(CASE
                              WHEN s.revenue_code BETWEEN '960' AND '989' THEN s.charge_amount
                          END, CAST(0 AS NUMERIC))) AS rasrv_charge_amt_cat46,
             sum(coalesce(CASE
                              WHEN s.revenue_code BETWEEN '300' AND '319'
                                   AND upper(rtrim(s.procedure_code)) <> '0' THEN s.charge_amount
                          END, CAST(0 AS NUMERIC))) AS rasrv_charge_amt_cat49,
             sum(coalesce(CASE
                              WHEN s.revenue_code BETWEEN '300' AND '319'
                                   AND upper(rtrim(s.procedure_code)) <> '0'
                                   AND s.procedure_code IS NOT NULL
                                   AND si.srv_identifirer_id IS NULL THEN s.provider_payment_amount
                          END, CAST(0 AS NUMERIC))) AS rasrv_prov_pymnt_amt_cat50,
             sum(coalesce(CASE
                              WHEN s.revenue_code BETWEEN '420' AND '449'
                                   AND upper(rtrim(s.procedure_code)) <> '0' THEN s.provider_payment_amount
                          END, CAST(0 AS NUMERIC))) AS rasrv_prov_pymnt_amt_cat88,
             sum(coalesce(CASE
                              WHEN s.revenue_code BETWEEN '420' AND '449'
                                   AND upper(rtrim(s.procedure_code)) <> '0'
                                   AND s.procedure_code IS NOT NULL
                                   AND si.srv_identifirer_id IS NULL THEN s.provider_payment_amount
                          END, CAST(0 AS NUMERIC))) AS rasrv_prov_pymnt_amt_cat93,
             sum(coalesce(sadj.rasrvadj_pr_code, CAST(0 AS BIGNUMERIC))) AS rasrvadj_pr_code,
             sum(coalesce(sadj.rasrvadj_oa_code, CAST(0 AS BIGNUMERIC))) AS rasrvadj_oa_code
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_service AS s
      LEFT OUTER JOIN
        (SELECT i.ra_service_id,
                i.schema_id,
                max(i.id) AS srv_identifirer_id
         FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_service_identifier AS i
         WHERE upper(rtrim(i.identifier_type)) = '1S'
         GROUP BY 1,
                  2) AS si ON si.ra_service_id = s.id
      AND si.schema_id = s.schema_id
      LEFT OUTER JOIN
        (SELECT sa.ra_service_id,
                sa.schema_id,
                sum(coalesce(CASE
                                 WHEN upper(rtrim(sa.group_code)) = 'PR'
                                      AND upper(rtrim(sa.reason_code)) = '66' THEN sa.amount
                             END, CAST(0 AS NUMERIC))) AS rasrvadj_pr_code,
                sum(coalesce(CASE
                                 WHEN upper(rtrim(sa.group_code)) = 'OA'
                                      AND upper(rtrim(sa.reason_code)) IN('23', '71') THEN sa.amount
                             END, CAST(0 AS NUMERIC))) AS rasrvadj_oa_code
         FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_service_adjustment AS sa
         WHERE upper(rtrim(sa.group_code)) IN('PR',
                                              'OA')
           AND upper(rtrim(sa.reason_code)) IN('66',
                                               '23',
                                               '71')
         GROUP BY 1,
                  2) AS sadj ON sadj.ra_service_id = s.id
      AND sadj.schema_id = s.schema_id
      GROUP BY 1,
               2) AS rasrv ON rasrv.ra_claim_payment_id = racp.id
   AND rasrv.schema_id = racp.schema_id
   GROUP BY upper(reforg.company_code),
            upper(CASE
                      WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                      ELSE og.client_id
                  END),
            3,
            4,
            upper(substr(substr(trim(format('%#34.0f', racp.ra_group_header_id)), length(trim(format('%#34.0f', racp.ra_group_header_id))) - 4, 4), 1, 4)),
            6,
            7,
            upper(substr(concat('INS', trim(format('%11d', CAST(mpyr.payer_rank AS INT64)))), 1, 14)),
            upper(og.short_name),
            11,
            12,
            13) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (upper(coalesce(mt.remittance_advice_num, '0')) = upper(coalesce(ms.remittance_advice_num, '0'))
     AND upper(coalesce(mt.remittance_advice_num, '1')) = upper(coalesce(ms.remittance_advice_num, '1')))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_claim_payment_id, NUMERIC '0') = coalesce(ms.rcpid, NUMERIC '0')
     AND coalesce(mt.ra_claim_payment_id, NUMERIC '1') = coalesce(ms.rcpid, NUMERIC '1'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.ra_pat_acct_num, NUMERIC '0') = coalesce(ms.ra_pat_acct_num, NUMERIC '0')
     AND coalesce(mt.ra_pat_acct_num, NUMERIC '1') = coalesce(ms.ra_pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_insurance_order_num, 0) = coalesce(ms.iplan_insurance_order_num, 0)
     AND coalesce(mt.iplan_insurance_order_num, 1) = coalesce(ms.iplan_insurance_order_num, 1))
AND (coalesce(mt.ra_iplan_id, 0) = coalesce(ms.ra_iplan_id, 0)
     AND coalesce(mt.ra_iplan_id, 1) = coalesce(ms.ra_iplan_id, 1))
AND (upper(coalesce(mt.rate_schedule_name, '0')) = upper(coalesce(ms.rate_schedule_name, '0'))
     AND upper(coalesce(mt.rate_schedule_name, '1')) = upper(coalesce(ms.rate_schedule_name, '1')))
AND (upper(coalesce(mt.financial_class, '0')) = upper(coalesce(ms.financial_class, '0'))
     AND upper(coalesce(mt.financial_class, '1')) = upper(coalesce(ms.financial_class, '1')))
AND (upper(coalesce(mt.ip_op_ind, '0')) = upper(coalesce(ms.ip_op_ind, '0'))
     AND upper(coalesce(mt.ip_op_ind, '1')) = upper(coalesce(ms.ip_op_ind, '1')))
AND (coalesce(mt.claim_ppscapital_amount, NUMERIC '0') = coalesce(ms.claim_ppscapital_amount, NUMERIC '0')
     AND coalesce(mt.claim_ppscapital_amount, NUMERIC '1') = coalesce(ms.claim_ppscapital_amount, NUMERIC '1'))
AND (coalesce(mt.claim_pps_capital_outlier_amnt, NUMERIC '0') = coalesce(ms.claim_pps_capital_outlier_amnt, NUMERIC '0')
     AND coalesce(mt.claim_pps_capital_outlier_amnt, NUMERIC '1') = coalesce(ms.claim_pps_capital_outlier_amnt, NUMERIC '1'))
AND (coalesce(mt.pps_operat_outlier_amount, NUMERIC '0') = coalesce(ms.pps_operat_outlier_amount, NUMERIC '0')
     AND coalesce(mt.pps_operat_outlier_amount, NUMERIC '1') = coalesce(ms.pps_operat_outlier_amount, NUMERIC '1'))
AND (coalesce(mt.claim_hcpcs_payable_amount, NUMERIC '0') = coalesce(ms.claim_hcpcs_payable_amount, NUMERIC '0')
     AND coalesce(mt.claim_hcpcs_payable_amount, NUMERIC '1') = coalesce(ms.claim_hcpcs_payable_amount, NUMERIC '1'))
AND (coalesce(mt.claim_payment_amount, NUMERIC '0') = coalesce(ms.claim_payment_amount, NUMERIC '0')
     AND coalesce(mt.claim_payment_amount, NUMERIC '1') = coalesce(ms.claim_payment_amount, NUMERIC '1'))
AND (coalesce(mt.claim_drg_amount, NUMERIC '0') = coalesce(ms.claim_drg_amount, NUMERIC '0')
     AND coalesce(mt.claim_drg_amount, NUMERIC '1') = coalesce(ms.claim_drg_amount, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_110, NUMERIC '0') = coalesce(ms.ra835_cat_110, NUMERIC '0')
     AND coalesce(mt.ra835_cat_110, NUMERIC '1') = coalesce(ms.ra835_cat_110, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_510, NUMERIC '0') = coalesce(ms.ra835_cat_510, NUMERIC '0')
     AND coalesce(mt.ra835_cat_510, NUMERIC '1') = coalesce(ms.ra835_cat_510, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_520, NUMERIC '0') = coalesce(ms.ra835_cat_520, NUMERIC '0')
     AND coalesce(mt.ra835_cat_520, NUMERIC '1') = coalesce(ms.ra835_cat_520, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_550, NUMERIC '0') = coalesce(ms.ra835_cat_550, NUMERIC '0')
     AND coalesce(mt.ra835_cat_550, NUMERIC '1') = coalesce(ms.ra835_cat_550, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_301_399, NUMERIC '0') = coalesce(ms.ra835_cat_301_399, NUMERIC '0')
     AND coalesce(mt.ra835_cat_301_399, NUMERIC '1') = coalesce(ms.ra835_cat_301_399, NUMERIC '1'))
AND (coalesce(mt.ra835_cat_501_599, NUMERIC '0') = coalesce(ms.ra835_cat_501_599, NUMERIC '0')
     AND coalesce(mt.ra835_cat_501_599, NUMERIC '1') = coalesce(ms.ra835_cat_501_599, NUMERIC '1'))
AND (coalesce(mt.rasrvadj_pr_code, NUMERIC '0') = coalesce(ms.rasrvadj_pr_code, NUMERIC '0')
     AND coalesce(mt.rasrvadj_pr_code, NUMERIC '1') = coalesce(ms.rasrvadj_pr_code, NUMERIC '1'))
AND (coalesce(mt.rasrvadj_oa_code, NUMERIC '0') = coalesce(ms.rasrvadj_oa_code, NUMERIC '0')
     AND coalesce(mt.rasrvadj_oa_code, NUMERIC '1') = coalesce(ms.rasrvadj_oa_code, NUMERIC '1'))
AND (coalesce(mt.raclaimadj_pr_code, NUMERIC '0') = coalesce(ms.raclaimadj_pr_code, NUMERIC '0')
     AND coalesce(mt.raclaimadj_pr_code, NUMERIC '1') = coalesce(ms.raclaimadj_pr_code, NUMERIC '1'))
AND (coalesce(mt.raclaimadj_oa_code, NUMERIC '0') = coalesce(ms.raclaimadj_oa_code, NUMERIC '0')
     AND coalesce(mt.raclaimadj_oa_code, NUMERIC '1') = coalesce(ms.raclaimadj_oa_code, NUMERIC '1'))
AND (coalesce(mt.raclaimsup_amt_i_t, NUMERIC '0') = coalesce(ms.raclaimsup_amt_i_t, NUMERIC '0')
     AND coalesce(mt.raclaimsup_amt_i_t, NUMERIC '1') = coalesce(ms.raclaimsup_amt_i_t, NUMERIC '1'))
AND (coalesce(mt.raclaimsup_amt_zm, NUMERIC '0') = coalesce(ms.raclaimsup_amt_zm, NUMERIC '0')
     AND coalesce(mt.raclaimsup_amt_zm, NUMERIC '1') = coalesce(ms.raclaimsup_amt_zm, NUMERIC '1'))
AND (coalesce(mt.rasrv_charge_amt_cat46, NUMERIC '0') = coalesce(ms.rasrv_charge_amt_cat46, NUMERIC '0')
     AND coalesce(mt.rasrv_charge_amt_cat46, NUMERIC '1') = coalesce(ms.rasrv_charge_amt_cat46, NUMERIC '1'))
AND (coalesce(mt.rasrv_charge_amt_cat49, NUMERIC '0') = coalesce(ms.rasrv_charge_amt_cat49, NUMERIC '0')
     AND coalesce(mt.rasrv_charge_amt_cat49, NUMERIC '1') = coalesce(ms.rasrv_charge_amt_cat49, NUMERIC '1'))
AND (coalesce(mt.rasrv_prov_pymnt_amt_cat50, NUMERIC '0') = coalesce(ms.rasrv_prov_pymnt_amt_cat50, NUMERIC '0')
     AND coalesce(mt.rasrv_prov_pymnt_amt_cat50, NUMERIC '1') = coalesce(ms.rasrv_prov_pymnt_amt_cat50, NUMERIC '1'))
AND (coalesce(mt.rasrv_prov_pymnt_amt_cat88, NUMERIC '0') = coalesce(ms.rasrv_prov_pymnt_amt_cat88, NUMERIC '0')
     AND coalesce(mt.rasrv_prov_pymnt_amt_cat88, NUMERIC '1') = coalesce(ms.rasrv_prov_pymnt_amt_cat88, NUMERIC '1'))
AND (coalesce(mt.rasrv_prov_pymnt_amt_cat93, NUMERIC '0') = coalesce(ms.rasrv_prov_pymnt_amt_cat93, NUMERIC '0')
     AND coalesce(mt.rasrv_prov_pymnt_amt_cat93, NUMERIC '1') = coalesce(ms.rasrv_prov_pymnt_amt_cat93, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        ra_claim_payment_id,
        log_id,
        log_sequence_num,
        unit_num,
        ra_pat_acct_num,
        iplan_insurance_order_num,
        ra_iplan_id,
        rate_schedule_name,
        financial_class,
        ip_op_ind,
        claim_ppscapital_amount,
        claim_pps_capital_outlier_amnt,
        pps_operat_outlier_amount,
        claim_hcpcs_payable_amount,
        claim_payment_amount,
        claim_drg_amount,
        ra835_cat_110,
        ra835_cat_510,
        ra835_cat_520,
        ra835_cat_550,
        ra835_cat_301_399,
        ra835_cat_501_599,
        rasrvadj_pr_code,
        rasrvadj_oa_code,
        raclaimadj_pr_code,
        raclaimadj_oa_code,
        raclaimsup_amt_i_t,
        raclaimsup_amt_zm,
        rasrv_charge_amt_cat46,
        rasrv_charge_amt_cat49,
        rasrv_prov_pymnt_amt_cat50,
        rasrv_prov_pymnt_amt_cat88,
        rasrv_prov_pymnt_amt_cat93,
        dw_last_update_date_time)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.remittance_advice_num, ms.ra_log_date, ms.rcpid, ms.log_id, ms.log_sequence_num, ms.unit_num, ms.ra_pat_acct_num, ms.iplan_insurance_order_num, ms.ra_iplan_id, ms.rate_schedule_name, ms.financial_class, ms.ip_op_ind, ms.claim_ppscapital_amount, ms.claim_pps_capital_outlier_amnt, ms.pps_operat_outlier_amount, ms.claim_hcpcs_payable_amount, ms.claim_payment_amount, ms.claim_drg_amount, ms.ra835_cat_110, ms.ra835_cat_510, ms.ra835_cat_520, ms.ra835_cat_550, ms.ra835_cat_301_399, ms.ra835_cat_501_599, ms.rasrvadj_pr_code, ms.rasrvadj_oa_code, ms.raclaimadj_pr_code, ms.raclaimadj_oa_code, ms.raclaimsup_amt_i_t, ms.raclaimsup_amt_zm, ms.rasrv_charge_amt_cat46, ms.rasrv_charge_amt_cat49, ms.rasrv_prov_pymnt_amt_cat50, ms.rasrv_prov_pymnt_amt_cat88, ms.rasrv_prov_pymnt_amt_cat93, ms.dw_last_update_date_time);


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
             log_sequence_num
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               remittance_advice_num,
               ra_log_date,
               log_id,
               log_sequence_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_remit_amt_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;