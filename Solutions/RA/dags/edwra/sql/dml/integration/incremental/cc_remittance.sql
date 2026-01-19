DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_remittance.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***********************************************************************************************************************
   Developer: Holly Ray
        Date: 8/11/2011
        Name: CC_Remittance.sql
        Mod1: Initial creation of BTEQ script on 8/11/2011. Replaces CC_Actual_Reimbursement
              due to revised Remit Model.
        Mod2: Removed logic deriving data from reason codes to use RA_Category_Aggregated table on
              1/9/2013 SW due to revised Remit Model.
        Mod3: Fixed defects for co-insurance and outlier_ind poplutation found in QA. New requirement to use
              RA_Claim_Payment Covered_Days_Visits_Count when Actual_Covered_Days IS NULL on 2/18/2013 SW.
        Mod4: Fixed defects for RA_Net_Benefit_Amt calculations on 3/6/2013 SW.
        Mod5: Defect fix for coinsurance amount discovered on 4/16/2013. Should use category 510, not 550 on 4/17/2013 SW.
        Mod6: Tuned SQL for performance. Removed diagnostics on 11/5/2014 SW.
	    Mod7: Removed CAST on Patient Account number on 1/14/2015 AS.
	    Mod8: Optimized SQL on 2/6/2015 SW.
	    Mod9: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
		Mod10:Changed delete to only consider active coids on 1/30/2018 SW.
		  Mod11: Optimized script to add temp table  6/12/2018  PT
	Mod12:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod13:  -  PBI 25628  - 3/23/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA131;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nohashjoin on for session;
 -- Locking EDWPF_STAGING.Payor_Organization for Access
BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_merge_stage;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_merge_stage AS mt USING
  (SELECT DISTINCT company_cd AS company_cd,
                   coido AS coido,
                   a.patient_dw_id AS patient_dw_id,
                   pyro.payor_dw_id AS payor_dw_id,
                   remittance_advice_num AS remittance_advice_num,
                   ra_log_date AS ra_log_date,
                   substr(CASE
                              WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
                              ELSE format('%4d', 0)
                          END, 1, 4) AS log_id,
                   row_number() OVER (PARTITION BY upper(company_cd),
                                                   upper(coido),
                                                   a.patient_dw_id,
                                                   pyro.payor_dw_id,
                                                   ra_log_date,
                                                   upper(pyro.log_id),
                                                   upper(remittance_advice_num)
                                      ORDER BY racp.id) AS log_sequence_num,
                                     ragh.id AS remittance_header_id,
                                     racp.id AS remittance_id,
                                     og.short_name AS unit_num,
                                     substr(format('%#14.0f', ma.account_no), 1, 38) AS ra_pat_acct_num,
                                     mpyr.payer_rank AS iplan_insurance_order_num,
                                     CASE
                                         WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                                         ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                                     END AS ra_iplan_id,
                                     racice.identifier_value AS ra_ep_iplan_id,
                                     CASE
                                         WHEN racp.service_end_date IS NOT NULL THEN racp.service_end_date
                                         ELSE DATE '1800-01-01'
                                     END AS ar_bill_thru_date,
                                     racp.drg_code AS ra_drg_code,
                                     racp.drg_weight AS ra_drg_weight,
                                     rasvcode.procedure_code AS ra_hipps_code,
                                     CAST(ROUND(coalesce(racp.actual_covered_days, racp.covered_days_visits_count), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_covered_days_num,
                                     racp.visits AS ra_visit_cnt,
                                     racp.covered_days_visits_count AS ra_covered_days_visit_cnt,
                                     substr(CASE
                                                WHEN upper(rtrim(mpt.ip_op_ind)) = 'O'
                                                     AND coalesce(racs.raclaimsupplementamt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
                                                WHEN upper(rtrim(mpt.ip_op_ind)) = 'I'
                                                     AND coalesce(racp.pps_operat_outlier_amount, CAST(0 AS NUMERIC)) <> 0
                                                     OR coalesce(racp.claim_pps_capital_outlier_amnt, CAST(0 AS NUMERIC)) <> 0 THEN 'C'
                                                ELSE ''
                                            END, 1, 2) AS outlier_ind,
                                     racp.discharge_fraction AS ra_discharge_fraction_pct,
                                     racp.reinbursment_rate AS ra_reimbursement_rate_pct,
                                     racp.not_replaced_blood_units AS ra_non_replaced_blood_unit_qty,
                                     racp.prescription AS ra_prescription_qty,
                                     racp.ttl_claim_charge_amount AS ra_total_charge_amt,
                                     ra_non_covered_charge_amt AS ra_non_covered_charge_amt,
                                     ra_net_billed_charge_amt AS ra_net_billed_charge_amt,
                                     CAST(ROUND(coalesce(raaggcat550.raaggcat550amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_deductible_amt,
                                     CAST(ROUND(coalesce(raaggcat510.raaggcat510amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_coinsurance_amt,
                                     CAST(ROUND(CASE
                                                    WHEN mpyr.payer_rank = 1 THEN coalesce(racp.ttl_claim_charge_amount, CAST(0 AS NUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)) - coalesce(racp.claim_payment_amount, CAST(0 AS NUMERIC))
                                                    ELSE coalesce(ROUND(ra_net_billed_charge_amt, 2, 'ROUND_HALF_EVEN'), CAST(0 AS BIGNUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC))
                                                END, 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_net_benefit_amt,
                                     racp.claim_payment_amount AS ra_insurance_payment_amt,
                                     CAST(ROUND(coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_patient_responsible_amt,
                                     racp.patient_code_qualifier AS ra_patient_qualifier_code,
                                     raci.identifier_value AS ra_claim_status_code,
                                     racp.patient_status_code AS ra_patient_status_code,
                                     racp.claim_filing_indicat_code AS ra_claim_filing_ind_code,
                                     racp.mia_moa_type AS ra_medicare_format_code,
                                     racp.facility_type_code AS ra_facility_type_code,
                                     racp.claim_frequency_code AS ra_claim_frequency_code,
                                     racp.payer_claim_control_no AS ra_payer_claim_control_num,
                                     CASE
                                         WHEN racp.received_date IS NOT NULL THEN racp.received_date
                                         ELSE DATE '1800-01-01'
                                     END AS ra_receive_date,
                                     racp.service_start_date AS ra_service_start_date,
                                     racp.service_end_date AS ra_service_end_date,
                                     racp.coverage_expiration_date AS ra_coverage_expiration_date,
                                     racp.patient_id AS ra_payer_patient_id,
                                     racp.member_id AS ra_payer_member_id,
                                     racp.replaced_by_ra_id AS ra_replaced_by_remit_id,
                                     racp.source_type_id AS ra_source_type_id,
                                     substr(CASE
                                                WHEN racp.is_deleted = 1 THEN 'N'
                                                ELSE 'Y'
                                            END, 1, 2) AS active_ind,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                                     'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat400amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 400 AND 499
      GROUP BY 2,
               3) AS raaggcat400 ON racp.id = raaggcat400.ra_claim_payment_id
   AND racp.schema_id = raaggcat400.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat500amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 500 AND 599
      GROUP BY 2,
               3) AS raaggcat500 ON racp.id = raaggcat500.ra_claim_payment_id
   AND racp.schema_id = raaggcat500.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat510amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 510
      GROUP BY 2,
               3) AS raaggcat510 ON racp.id = raaggcat510.ra_claim_payment_id
   AND racp.schema_id = raaggcat510.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat550amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 550
      GROUP BY 2,
               3) AS raaggcat550 ON racp.id = raaggcat550.ra_claim_payment_id
   AND racp.schema_id = raaggcat550.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_claim_supplement.supplement_amount, CAST(0 AS NUMERIC))) AS raclaimsupplementamt,
             ra_claim_supplement.ra_claim_payment_id,
             ra_claim_supplement.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_supplement
      WHERE upper(rtrim(ra_claim_supplement.supplement_type)) = 'ZM'
      GROUP BY 2,
               3) AS racs ON racp.id = racs.ra_claim_payment_id
   AND racp.schema_id = racs.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
   AND ragh.schema_id = racp.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
   AND racp.schema_id = ma.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON mpt.schema_id = ma.schema_id
   AND mpt.id = ma.mon_patient_type_id
   AND mpt.org_id = ma.org_id_provider
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
   AND racp.schema_id = mpyr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
   AND pyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT max(ra_service.procedure_code) AS procedure_code,
             ra_service.ra_claim_payment_id,
             ra_service.schema_id
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.ra_service
      WHERE upper(rtrim(ra_service.procedure_code_type)) = 'ZZ'
      GROUP BY 2,
               3,
               upper(ra_service.procedure_code)) AS rasvcode ON racp.id = rasvcode.ra_claim_payment_id
   AND racp.schema_id = rasvcode.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS raci ON raci.ra_claim_payment_id = racp.id
   AND raci.schema_id = racp.schema_id
   AND upper(rtrim(raci.identifier_type)) = '9C'
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS racice ON racice.ra_claim_payment_id = racp.id
   AND racice.schema_id = racp.schema_id
   AND upper(rtrim(racice.identifier_type)) = 'CE'
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS reforg ON upper(rtrim(reforg.coid)) = upper(rtrim(substr(og.client_id, 7, 5)))
   AND reforg.schema_id = og.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(reforg.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(pyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(pyr.code), 1, 3), substr(trim(pyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(reforg.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(reforg.company_code))
   AND a.pat_acct_num = ma.account_no
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(raaggcat400.raaggcat400amount, CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS ra_non_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ CAST(ROUND(coalesce(racp.ttl_claim_charge_amount - ROUND(ra_non_covered_charge_amt, 2, 'ROUND_HALF_EVEN'), CAST(0 AS BIGNUMERIC)), 2, 'ROUND_HALF_EVEN') AS NUMERIC) ]) AS ra_net_billed_charge_amt
   CROSS JOIN UNNEST(ARRAY[ substr(substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4), 1, 8) ]) AS remittance_advice_num
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN racp.date_created IS NOT NULL THEN racp.date_created
                                ELSE DATE '1800-01-01'
                            END ]) AS ra_log_date
   CROSS JOIN UNNEST(ARRAY[ CASE
                                WHEN upper(rtrim(substr(og.client_id, 1, 5))) = 'COID:' THEN substr(og.client_id, 7, 5)
                                ELSE og.client_id
                            END ]) AS coido
   CROSS JOIN UNNEST(ARRAY[ reforg.company_code ]) AS company_cd) AS ms ON upper(coalesce(mt.company_cd, '0')) = upper(coalesce(ms.company_cd, '0'))
AND upper(coalesce(mt.company_cd, '1')) = upper(coalesce(ms.company_cd, '1'))
AND (upper(coalesce(mt.coido, '0')) = upper(coalesce(ms.coido, '0'))
     AND upper(coalesce(mt.coido, '1')) = upper(coalesce(ms.coido, '1')))
AND (coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
     AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1'))
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (upper(coalesce(mt.remittance_advice_num, '0')) = upper(coalesce(ms.remittance_advice_num, '0'))
     AND upper(coalesce(mt.remittance_advice_num, '1')) = upper(coalesce(ms.remittance_advice_num, '1')))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.remittance_header_id, NUMERIC '0') = coalesce(ms.remittance_header_id, NUMERIC '0')
     AND coalesce(mt.remittance_header_id, NUMERIC '1') = coalesce(ms.remittance_header_id, NUMERIC '1'))
AND (coalesce(mt.remittance_id, NUMERIC '0') = coalesce(ms.remittance_id, NUMERIC '0')
     AND coalesce(mt.remittance_id, NUMERIC '1') = coalesce(ms.remittance_id, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (upper(coalesce(mt.ra_pat_acct_num, '0')) = upper(coalesce(ms.ra_pat_acct_num, '0'))
     AND upper(coalesce(mt.ra_pat_acct_num, '1')) = upper(coalesce(ms.ra_pat_acct_num, '1')))
AND (coalesce(mt.iplan_insurance_order_num, NUMERIC '0') = coalesce(ms.iplan_insurance_order_num, NUMERIC '0')
     AND coalesce(mt.iplan_insurance_order_num, NUMERIC '1') = coalesce(ms.iplan_insurance_order_num, NUMERIC '1'))
AND (coalesce(mt.ra_iplan_id, 0) = coalesce(ms.ra_iplan_id, 0)
     AND coalesce(mt.ra_iplan_id, 1) = coalesce(ms.ra_iplan_id, 1))
AND (upper(coalesce(mt.ra_ep_iplan_id, '0')) = upper(coalesce(ms.ra_ep_iplan_id, '0'))
     AND upper(coalesce(mt.ra_ep_iplan_id, '1')) = upper(coalesce(ms.ra_ep_iplan_id, '1')))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.ra_drg_code, '0')) = upper(coalesce(ms.ra_drg_code, '0'))
     AND upper(coalesce(mt.ra_drg_code, '1')) = upper(coalesce(ms.ra_drg_code, '1')))
AND (coalesce(mt.ra_drg_weight, NUMERIC '0') = coalesce(ms.ra_drg_weight, NUMERIC '0')
     AND coalesce(mt.ra_drg_weight, NUMERIC '1') = coalesce(ms.ra_drg_weight, NUMERIC '1'))
AND (upper(coalesce(mt.ra_hipps_code, '0')) = upper(coalesce(ms.ra_hipps_code, '0'))
     AND upper(coalesce(mt.ra_hipps_code, '1')) = upper(coalesce(ms.ra_hipps_code, '1')))
AND (coalesce(mt.ra_covered_days_num, NUMERIC '0') = coalesce(ms.ra_covered_days_num, NUMERIC '0')
     AND coalesce(mt.ra_covered_days_num, NUMERIC '1') = coalesce(ms.ra_covered_days_num, NUMERIC '1'))
AND (coalesce(mt.ra_visit_cnt, NUMERIC '0') = coalesce(ms.ra_visit_cnt, NUMERIC '0')
     AND coalesce(mt.ra_visit_cnt, NUMERIC '1') = coalesce(ms.ra_visit_cnt, NUMERIC '1'))
AND (coalesce(mt.ra_covered_days_visit_cnt, NUMERIC '0') = coalesce(ms.ra_covered_days_visit_cnt, NUMERIC '0')
     AND coalesce(mt.ra_covered_days_visit_cnt, NUMERIC '1') = coalesce(ms.ra_covered_days_visit_cnt, NUMERIC '1'))
AND (upper(coalesce(mt.outlier_ind, '0')) = upper(coalesce(ms.outlier_ind, '0'))
     AND upper(coalesce(mt.outlier_ind, '1')) = upper(coalesce(ms.outlier_ind, '1')))
AND (coalesce(mt.ra_discharge_fraction_pct, NUMERIC '0') = coalesce(ms.ra_discharge_fraction_pct, NUMERIC '0')
     AND coalesce(mt.ra_discharge_fraction_pct, NUMERIC '1') = coalesce(ms.ra_discharge_fraction_pct, NUMERIC '1'))
AND (coalesce(mt.ra_reimbursement_rate_pct, NUMERIC '0') = coalesce(ms.ra_reimbursement_rate_pct, NUMERIC '0')
     AND coalesce(mt.ra_reimbursement_rate_pct, NUMERIC '1') = coalesce(ms.ra_reimbursement_rate_pct, NUMERIC '1'))
AND (coalesce(mt.ra_non_replaced_blood_unit_qty, NUMERIC '0') = coalesce(ms.ra_non_replaced_blood_unit_qty, NUMERIC '0')
     AND coalesce(mt.ra_non_replaced_blood_unit_qty, NUMERIC '1') = coalesce(ms.ra_non_replaced_blood_unit_qty, NUMERIC '1'))
AND (coalesce(mt.ra_prescription_qty, NUMERIC '0') = coalesce(ms.ra_prescription_qty, NUMERIC '0')
     AND coalesce(mt.ra_prescription_qty, NUMERIC '1') = coalesce(ms.ra_prescription_qty, NUMERIC '1'))
AND (coalesce(mt.ra_total_charge_amt, NUMERIC '0') = coalesce(ms.ra_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_charge_amt, NUMERIC '1') = coalesce(ms.ra_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_billed_charge_amt, NUMERIC '0') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_billed_charge_amt, NUMERIC '1') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_deductible_amt, NUMERIC '0') = coalesce(ms.ra_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_amt, NUMERIC '1') = coalesce(ms.ra_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_benefit_amt, NUMERIC '0') = coalesce(ms.ra_net_benefit_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_benefit_amt, NUMERIC '1') = coalesce(ms.ra_net_benefit_amt, NUMERIC '1'))
AND (coalesce(mt.ra_insurance_payment_amt, NUMERIC '0') = coalesce(ms.ra_insurance_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_insurance_payment_amt, NUMERIC '1') = coalesce(ms.ra_insurance_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_patient_responsible_amt, NUMERIC '0') = coalesce(ms.ra_patient_responsible_amt, NUMERIC '0')
     AND coalesce(mt.ra_patient_responsible_amt, NUMERIC '1') = coalesce(ms.ra_patient_responsible_amt, NUMERIC '1'))
AND (upper(coalesce(mt.ra_patient_qualifier_code, '0')) = upper(coalesce(ms.ra_patient_qualifier_code, '0'))
     AND upper(coalesce(mt.ra_patient_qualifier_code, '1')) = upper(coalesce(ms.ra_patient_qualifier_code, '1')))
AND (upper(coalesce(mt.ra_claim_status_code, '0')) = upper(coalesce(ms.ra_claim_status_code, '0'))
     AND upper(coalesce(mt.ra_claim_status_code, '1')) = upper(coalesce(ms.ra_claim_status_code, '1')))
AND (upper(coalesce(mt.ra_patient_status_code, '0')) = upper(coalesce(ms.ra_patient_status_code, '0'))
     AND upper(coalesce(mt.ra_patient_status_code, '1')) = upper(coalesce(ms.ra_patient_status_code, '1')))
AND (upper(coalesce(mt.ra_claim_filing_ind_code, '0')) = upper(coalesce(ms.ra_claim_filing_ind_code, '0'))
     AND upper(coalesce(mt.ra_claim_filing_ind_code, '1')) = upper(coalesce(ms.ra_claim_filing_ind_code, '1')))
AND (upper(coalesce(mt.ra_medicare_format_code, '0')) = upper(coalesce(ms.ra_medicare_format_code, '0'))
     AND upper(coalesce(mt.ra_medicare_format_code, '1')) = upper(coalesce(ms.ra_medicare_format_code, '1')))
AND (upper(coalesce(mt.ra_facility_type_code, '0')) = upper(coalesce(ms.ra_facility_type_code, '0'))
     AND upper(coalesce(mt.ra_facility_type_code, '1')) = upper(coalesce(ms.ra_facility_type_code, '1')))
AND (upper(coalesce(mt.ra_claim_frequency_code, '0')) = upper(coalesce(ms.ra_claim_frequency_code, '0'))
     AND upper(coalesce(mt.ra_claim_frequency_code, '1')) = upper(coalesce(ms.ra_claim_frequency_code, '1')))
AND (upper(coalesce(mt.ra_payer_claim_control_num, '0')) = upper(coalesce(ms.ra_payer_claim_control_num, '0'))
     AND upper(coalesce(mt.ra_payer_claim_control_num, '1')) = upper(coalesce(ms.ra_payer_claim_control_num, '1')))
AND (coalesce(mt.ra_receive_date, DATE '1970-01-01') = coalesce(ms.ra_receive_date, DATE '1970-01-01')
     AND coalesce(mt.ra_receive_date, DATE '1970-01-02') = coalesce(ms.ra_receive_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_service_start_date, DATE '1970-01-01') = coalesce(ms.ra_service_start_date, DATE '1970-01-01')
     AND coalesce(mt.ra_service_start_date, DATE '1970-01-02') = coalesce(ms.ra_service_start_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_service_end_date, DATE '1970-01-01') = coalesce(ms.ra_service_end_date, DATE '1970-01-01')
     AND coalesce(mt.ra_service_end_date, DATE '1970-01-02') = coalesce(ms.ra_service_end_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_coverage_expiration_date, DATE '1970-01-01') = coalesce(ms.ra_coverage_expiration_date, DATE '1970-01-01')
     AND coalesce(mt.ra_coverage_expiration_date, DATE '1970-01-02') = coalesce(ms.ra_coverage_expiration_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.ra_payer_patient_id, '0')) = upper(coalesce(ms.ra_payer_patient_id, '0'))
     AND upper(coalesce(mt.ra_payer_patient_id, '1')) = upper(coalesce(ms.ra_payer_patient_id, '1')))
AND (upper(coalesce(mt.ra_payer_member_id, '0')) = upper(coalesce(ms.ra_payer_member_id, '0'))
     AND upper(coalesce(mt.ra_payer_member_id, '1')) = upper(coalesce(ms.ra_payer_member_id, '1')))
AND (coalesce(mt.ra_replaced_by_remit_id, NUMERIC '0') = coalesce(ms.ra_replaced_by_remit_id, NUMERIC '0')
     AND coalesce(mt.ra_replaced_by_remit_id, NUMERIC '1') = coalesce(ms.ra_replaced_by_remit_id, NUMERIC '1'))
AND (coalesce(mt.ra_source_type_id, NUMERIC '0') = coalesce(ms.ra_source_type_id, NUMERIC '0')
     AND coalesce(mt.ra_source_type_id, NUMERIC '1') = coalesce(ms.ra_source_type_id, NUMERIC '1'))
AND (upper(coalesce(mt.active_ind, '0')) = upper(coalesce(ms.active_ind, '0'))
     AND upper(coalesce(mt.active_ind, '1')) = upper(coalesce(ms.active_ind, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_cd,
        coido,
        patient_dw_id,
        payor_dw_id,
        remittance_advice_num,
        ra_log_date,
        log_id,
        log_sequence_num,
        remittance_header_id,
        remittance_id,
        unit_num,
        ra_pat_acct_num,
        iplan_insurance_order_num,
        ra_iplan_id,
        ra_ep_iplan_id,
        ar_bill_thru_date,
        ra_drg_code,
        ra_drg_weight,
        ra_hipps_code,
        ra_covered_days_num,
        ra_visit_cnt,
        ra_covered_days_visit_cnt,
        outlier_ind,
        ra_discharge_fraction_pct,
        ra_reimbursement_rate_pct,
        ra_non_replaced_blood_unit_qty,
        ra_prescription_qty,
        ra_total_charge_amt,
        ra_non_covered_charge_amt,
        ra_net_billed_charge_amt,
        ra_deductible_amt,
        ra_coinsurance_amt,
        ra_net_benefit_amt,
        ra_insurance_payment_amt,
        ra_patient_responsible_amt,
        ra_patient_qualifier_code,
        ra_claim_status_code,
        ra_patient_status_code,
        ra_claim_filing_ind_code,
        ra_medicare_format_code,
        ra_facility_type_code,
        ra_claim_frequency_code,
        ra_payer_claim_control_num,
        ra_receive_date,
        ra_service_start_date,
        ra_service_end_date,
        ra_coverage_expiration_date,
        ra_payer_patient_id,
        ra_payer_member_id,
        ra_replaced_by_remit_id,
        ra_source_type_id,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_cd, ms.coido, ms.patient_dw_id, ms.payor_dw_id, ms.remittance_advice_num, ms.ra_log_date, ms.log_id, ms.log_sequence_num, ms.remittance_header_id, ms.remittance_id, ms.unit_num, ms.ra_pat_acct_num, ms.iplan_insurance_order_num, ms.ra_iplan_id, ms.ra_ep_iplan_id, ms.ar_bill_thru_date, ms.ra_drg_code, ms.ra_drg_weight, ms.ra_hipps_code, ms.ra_covered_days_num, ms.ra_visit_cnt, ms.ra_covered_days_visit_cnt, ms.outlier_ind, ms.ra_discharge_fraction_pct, ms.ra_reimbursement_rate_pct, ms.ra_non_replaced_blood_unit_qty, ms.ra_prescription_qty, ms.ra_total_charge_amt, ms.ra_non_covered_charge_amt, ms.ra_net_billed_charge_amt, ms.ra_deductible_amt, ms.ra_coinsurance_amt, ms.ra_net_benefit_amt, ms.ra_insurance_payment_amt, ms.ra_patient_responsible_amt, ms.ra_patient_qualifier_code, ms.ra_claim_status_code, ms.ra_patient_status_code, ms.ra_claim_filing_ind_code, ms.ra_medicare_format_code, ms.ra_facility_type_code, ms.ra_claim_frequency_code, ms.ra_payer_claim_control_num, ms.ra_receive_date, ms.ra_service_start_date, ms.ra_service_end_date, ms.ra_coverage_expiration_date, ms.ra_payer_patient_id, ms.ra_payer_member_id, ms.ra_replaced_by_remit_id, ms.ra_source_type_id, ms.active_ind, ms.dw_last_update_date_time, ms.source_system_code);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra_staging','CC_Remittance_Merge_stage');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS x USING
  (SELECT cc_remittance_merge_stage.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_remittance_merge_stage) AS z ON x.patient_dw_id = z.patient_dw_id
AND upper(rtrim(x.company_code)) = upper(rtrim(z.company_cd))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coido))
AND x.payor_dw_id = z.payor_dw_id
AND x.remittance_advice_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.remittance_advice_num) AS FLOAT64)
AND x.ra_log_date = z.ra_log_date
AND upper(rtrim(x.log_id)) = upper(rtrim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num WHEN MATCHED THEN
UPDATE
SET remittance_header_id = z.remittance_header_id,
    remittance_id = z.remittance_id,
    unit_num = substr(z.unit_num, 1, 5),
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    iplan_insurance_order_num = CAST(z.iplan_insurance_order_num AS INT64),
    iplan_id = z.ra_iplan_id,
    ra_ep_iplan_id = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_ep_iplan_id) AS INT64),
    ar_bill_thru_date = z.ar_bill_thru_date,
    ra_drg_code = substr(z.ra_drg_code, 1, 5),
    ra_drg_weight = ROUND(z.ra_drg_weight, 5, 'ROUND_HALF_EVEN'),
    ra_hipps_code = substr(z.ra_hipps_code, 1, 5),
    ra_covered_days_num = CAST(z.ra_covered_days_num AS INT64),
    ra_visit_cnt = CAST(z.ra_visit_cnt AS INT64),
    ra_covered_days_visit_cnt = CAST(z.ra_covered_days_visit_cnt AS INT64),
    outlier_ind = substr(z.outlier_ind, 1, 1),
    ra_discharge_fraction_pct = ROUND(z.ra_discharge_fraction_pct, 4, 'ROUND_HALF_EVEN'),
    ra_reimbursement_rate_pct = ROUND(z.ra_reimbursement_rate_pct, 4, 'ROUND_HALF_EVEN'),
    ra_non_replaced_blood_unit_qty = z.ra_non_replaced_blood_unit_qty,
    ra_prescription_qty = z.ra_prescription_qty,
    ra_total_charge_amt = ROUND(z.ra_total_charge_amt, 3, 'ROUND_HALF_EVEN'),
    ra_non_covered_charge_amt = ROUND(z.ra_non_covered_charge_amt, 3, 'ROUND_HALF_EVEN'),
    ra_net_billed_charge_amt = ROUND(z.ra_net_billed_charge_amt, 3, 'ROUND_HALF_EVEN'),
    ra_deductible_amt = ROUND(z.ra_deductible_amt, 3, 'ROUND_HALF_EVEN'),
    ra_coinsurance_amt = ROUND(z.ra_coinsurance_amt, 3, 'ROUND_HALF_EVEN'),
    ra_net_benefit_amt = ROUND(z.ra_net_benefit_amt, 3, 'ROUND_HALF_EVEN'),
    ra_insurance_payment_amt = ROUND(z.ra_insurance_payment_amt, 3, 'ROUND_HALF_EVEN'),
    ra_patient_responsible_amt = ROUND(z.ra_patient_responsible_amt, 3, 'ROUND_HALF_EVEN'),
    ra_patient_qualifier_code = substr(z.ra_patient_qualifier_code, 1, 2),
    ra_claim_status_code = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_claim_status_code) AS INT64),
    ra_patient_status_code = substr(z.ra_patient_status_code, 1, 2),
    ra_claim_filing_ind_code = substr(z.ra_claim_filing_ind_code, 1, 2),
    ra_medicare_format_code = z.ra_medicare_format_code,
    ra_facility_type_code = substr(z.ra_facility_type_code, 1, 2),
    ra_claim_frequency_code = substr(z.ra_claim_frequency_code, 1, 1),
    ra_payer_claim_control_num = substr(z.ra_payer_claim_control_num, 1, 30),
    ra_receive_date = z.ra_receive_date,
    ra_service_start_date = z.ra_service_start_date,
    ra_service_end_date = z.ra_service_end_date,
    ra_coverage_expiration_date = z.ra_coverage_expiration_date,
    ra_payer_patient_id = substr(z.ra_payer_patient_id, 1, 100),
    ra_payer_member_id = substr(z.ra_payer_member_id, 1, 100),
    ra_replaced_by_remit_id = z.ra_replaced_by_remit_id,
    ra_source_type_id = z.ra_source_type_id,
    active_ind = substr(z.active_ind, 1, 1),
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
        remittance_header_id,
        remittance_id,
        unit_num,
        pat_acct_num,
        iplan_insurance_order_num,
        iplan_id,
        ra_ep_iplan_id,
        ar_bill_thru_date,
        ra_drg_code,
        ra_drg_weight,
        ra_hipps_code,
        ra_covered_days_num,
        ra_visit_cnt,
        ra_covered_days_visit_cnt,
        outlier_ind,
        ra_discharge_fraction_pct,
        ra_reimbursement_rate_pct,
        ra_non_replaced_blood_unit_qty,
        ra_prescription_qty,
        ra_total_charge_amt,
        ra_non_covered_charge_amt,
        ra_net_billed_charge_amt,
        ra_deductible_amt,
        ra_coinsurance_amt,
        ra_net_benefit_amt,
        ra_insurance_payment_amt,
        ra_patient_responsible_amt,
        ra_patient_qualifier_code,
        ra_claim_status_code,
        ra_patient_status_code,
        ra_claim_filing_ind_code,
        ra_medicare_format_code,
        ra_facility_type_code,
        ra_claim_frequency_code,
        ra_payer_claim_control_num,
        ra_receive_date,
        ra_service_start_date,
        ra_service_end_date,
        ra_coverage_expiration_date,
        ra_payer_patient_id,
        ra_payer_member_id,
        ra_replaced_by_remit_id,
        ra_source_type_id,
        active_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.company_cd, substr(z.coido, 1, 5), z.patient_dw_id, z.payor_dw_id, CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.remittance_advice_num) AS INT64), z.ra_log_date, substr(z.log_id, 1, 4), z.log_sequence_num, z.remittance_header_id, z.remittance_id, substr(z.unit_num, 1, 5), ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), CAST(z.iplan_insurance_order_num AS INT64), z.ra_iplan_id, CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_ep_iplan_id) AS INT64), z.ar_bill_thru_date, substr(z.ra_drg_code, 1, 5), ROUND(z.ra_drg_weight, 5, 'ROUND_HALF_EVEN'), substr(z.ra_hipps_code, 1, 5), CAST(z.ra_covered_days_num AS INT64), CAST(z.ra_visit_cnt AS INT64), CAST(z.ra_covered_days_visit_cnt AS INT64), substr(z.outlier_ind, 1, 1), ROUND(z.ra_discharge_fraction_pct, 4, 'ROUND_HALF_EVEN'), ROUND(z.ra_reimbursement_rate_pct, 4, 'ROUND_HALF_EVEN'), z.ra_non_replaced_blood_unit_qty, z.ra_prescription_qty, ROUND(z.ra_total_charge_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_non_covered_charge_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_net_billed_charge_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_deductible_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_coinsurance_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_net_benefit_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_insurance_payment_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ra_patient_responsible_amt, 3, 'ROUND_HALF_EVEN'), substr(z.ra_patient_qualifier_code, 1, 2), CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.ra_claim_status_code) AS INT64), substr(z.ra_patient_status_code, 1, 2), substr(z.ra_claim_filing_ind_code, 1, 2), z.ra_medicare_format_code, substr(z.ra_facility_type_code, 1, 2), substr(z.ra_claim_frequency_code, 1, 1), substr(z.ra_payer_claim_control_num, 1, 30), z.ra_receive_date, z.ra_service_start_date, z.ra_service_end_date, z.ra_coverage_expiration_date, substr(z.ra_payer_patient_id, 1, 100), substr(z.ra_payer_member_id, 1, 100), z.ra_replaced_by_remit_id, z.ra_source_type_id, substr(z.active_ind, 1, 1), datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


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
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance
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

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS a
WHERE upper(a.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND (upper(a.company_code),
       upper(a.coid),
       a.patient_dw_id,
       a.remittance_id) IN
    (SELECT AS STRUCT upper(max(b.company_code)) AS company_code,
                      upper(max(b.coid)) AS coid,
                      b.patient_dw_id,
                      b.remittance_id
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS b
     GROUP BY upper(b.company_code),
              upper(b.coid),
              3,
              4
     HAVING count(*) > 1)
  AND upper(CAST(a.dw_last_update_date_time AS STRING)) NOT IN
    (SELECT upper(CAST(max(c.dw_last_update_date_time) AS STRING))
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS c
     WHERE a.remittance_id = c.remittance_id );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','cc_remittance');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;