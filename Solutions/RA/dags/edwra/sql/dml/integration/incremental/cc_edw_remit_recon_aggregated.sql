DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_edw_remit_recon_aggregated.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- ************************************************************************** Developer: Tawale Pritam Name: CC_EDW_Remit_Recon_Aggregated - BTEQ Script. Date: Creation OF script ON 06/21/2016 ****************************************************************************/
 -- bteq << EOF > /etl/ST/CT/CC_EDW/TgtFiles/CC_EDW_Remit_Recon_Aggregated_P1.out;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM  {{ params.param_parallon_ra_stage_dataset_name }}.edw_remit_recon_aggregated
WHERE DATE(edw_remit_recon_aggregated.dw_last_update_date_time) = date_sub(current_date('US/Central'), interval 54 DAY);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO  {{ params.param_parallon_ra_stage_dataset_name }}.edw_remit_recon_aggregated (unit_num, ra_pat_acct_num, ra_covered_days_num, ra_visit_cnt, ra_covered_days_visit_cnt, ra_non_replaced_blood_unit_qty, ra_prescription_qty, ra_total_charge_amt, ra_non_covered_charge_amt, ra_net_billed_charge_amt, ra_deductible_amt, ra_coinsurance_amt, ra_net_benefit_amt, ra_insurance_payment_amt, ra_patient_responsible_amt, schema_id, dw_last_update_date_time)
SELECT substr(max(a.unit_num), 1, 5) AS unit_num,
       count(a.ra_pat_acct_num),
       CAST(sum(a.ra_covered_days_num) AS INT64),
       CAST(sum(a.ra_visit_cnt) AS INT64),
       CAST(sum(a.ra_covered_days_visit_cnt) AS INT64),
       CAST(sum(a.ra_non_replaced_blood_unit_qty) AS INT64),
       CAST(sum(a.ra_prescription_qty) AS INT64),
       CAST(ROUND(sum(a.ra_total_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_non_covered_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_net_billed_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_deductible_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_coinsurance_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_net_benefit_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_insurance_payment_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_patient_responsible_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       1 AS schema_id,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  (SELECT substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) AS remittance_advice_num,
          CASE
              WHEN racp.date_created IS NOT NULL THEN racp.date_created
              ELSE DATE '1800-01-01'
          END AS ra_log_date,
          CASE
              WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
              ELSE format('%4d', 0)
          END AS log_id,
          ragh.id AS remittance_header_id,
          racp.id AS remittance_id,
          og.short_name AS unit_num,
          ma.account_no AS ra_pat_acct_num,
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
          coalesce(racp.actual_covered_days, racp.covered_days_visits_count) AS ra_covered_days_num,
          racp.visits AS ra_visit_cnt,
          racp.covered_days_visits_count AS ra_covered_days_visit_cnt,
          CASE
              WHEN upper(rtrim(mpt.ip_op_ind)) = 'O'
                   AND coalesce(racs.raclaimsupplementamt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
              WHEN upper(rtrim(mpt.ip_op_ind)) = 'I'
                   AND coalesce(racp.pps_operat_outlier_amount, CAST(0 AS NUMERIC)) <> 0
                   OR coalesce(racp.claim_pps_capital_outlier_amnt, CAST(0 AS NUMERIC)) <> 0 THEN 'C'
              ELSE ''
          END AS outlier_ind,
          racp.discharge_fraction AS ra_discharge_fraction_pct,
          racp.reinbursment_rate AS ra_reimbursement_rate_pct,
          racp.not_replaced_blood_units AS ra_non_replaced_blood_unit_qty,
          racp.prescription AS ra_prescription_qty,
          racp.ttl_claim_charge_amount AS ra_total_charge_amt,
          ra_non_covered_charge_amt AS ra_non_covered_charge_amt,
          ra_net_billed_charge_amt AS ra_net_billed_charge_amt,
          coalesce(raaggcat550.raaggcat550amount, CAST(0 AS BIGNUMERIC)) AS ra_deductible_amt,
          coalesce(raaggcat510.raaggcat510amount, CAST(0 AS BIGNUMERIC)) AS ra_coinsurance_amt,
          CASE
              WHEN mpyr.payer_rank = 1 THEN coalesce(racp.ttl_claim_charge_amount, CAST(0 AS NUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)) - coalesce(racp.claim_payment_amount, CAST(0 AS NUMERIC))
              ELSE coalesce(ra_net_billed_charge_amt, CAST(0 AS BIGNUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC))
          END AS ra_net_benefit_amt,
          racp.claim_payment_amount AS ra_insurance_payment_amt,
          coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)) AS ra_patient_responsible_amt,
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
          CASE
              WHEN racp.is_deleted = 1 THEN 'N'
              ELSE 'Y'
          END AS active_ind
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat400amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 400 AND 499
      GROUP BY 2,
               3) AS raaggcat400 ON racp.id = raaggcat400.ra_claim_payment_id
   AND racp.schema_id = raaggcat400.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat500amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 500 AND 599
      GROUP BY 2,
               3) AS raaggcat500 ON racp.id = raaggcat500.ra_claim_payment_id
   AND racp.schema_id = raaggcat500.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat510amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 510
      GROUP BY 2,
               3) AS raaggcat510 ON racp.id = raaggcat510.ra_claim_payment_id
   AND racp.schema_id = raaggcat510.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat550amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 550
      GROUP BY 2,
               3) AS raaggcat550 ON racp.id = raaggcat550.ra_claim_payment_id
   AND racp.schema_id = raaggcat550.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_claim_supplement.supplement_amount, CAST(0 AS NUMERIC))) AS raclaimsupplementamt,
             ra_claim_supplement.ra_claim_payment_id,
             ra_claim_supplement.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_supplement
      WHERE upper(rtrim(ra_claim_supplement.supplement_type)) = 'ZM'
      GROUP BY 2,
               3) AS racs ON racp.id = racs.ra_claim_payment_id
   AND racp.schema_id = racs.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
   AND ragh.schema_id = racp.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
   AND racp.schema_id = ma.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON mpt.schema_id = ma.schema_id
   AND mpt.id = ma.mon_patient_type_id
   AND mpt.org_id = ma.org_id_provider
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
   AND racp.schema_id = mpyr.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
   AND pyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT max(ra_service.procedure_code) AS procedure_code,
             ra_service.ra_claim_payment_id,
             ra_service.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_service
      WHERE upper(rtrim(ra_service.procedure_code_type)) = 'ZZ'
      GROUP BY 2,
               3,
               upper(ra_service.procedure_code)) AS rasvcode ON racp.id = rasvcode.ra_claim_payment_id
   AND racp.schema_id = rasvcode.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS raci ON raci.ra_claim_payment_id = racp.id
   AND raci.schema_id = racp.schema_id
   AND upper(rtrim(raci.identifier_type)) = '9C'
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS racice ON racice.ra_claim_payment_id = racp.id
   AND racice.schema_id = racp.schema_id
   AND upper(rtrim(racice.identifier_type)) = 'CE'
   CROSS JOIN UNNEST(ARRAY[ coalesce(raaggcat400.raaggcat400amount, CAST(0 AS BIGNUMERIC)) ]) AS ra_non_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ coalesce(racp.ttl_claim_charge_amount - ra_non_covered_charge_amt, CAST(0 AS BIGNUMERIC)) ]) AS ra_net_billed_charge_amt
   WHERE racp.schema_id = 1
     AND racp.date_created < current_date('US/Central') ) AS a
GROUP BY upper(substr(a.unit_num, 1, 5));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


INSERT INTO  {{ params.param_parallon_ra_stage_dataset_name }}.edw_remit_recon_aggregated (unit_num, ra_pat_acct_num, ra_covered_days_num, ra_visit_cnt, ra_covered_days_visit_cnt, ra_non_replaced_blood_unit_qty, ra_prescription_qty, ra_total_charge_amt, ra_non_covered_charge_amt, ra_net_billed_charge_amt, ra_deductible_amt, ra_coinsurance_amt, ra_net_benefit_amt, ra_insurance_payment_amt, ra_patient_responsible_amt, schema_id, dw_last_update_date_time)
SELECT substr(max(a.unit_num), 1, 5) AS unit_num,
       count(a.ra_pat_acct_num),
       CAST(sum(a.ra_covered_days_num) AS INT64),
       CAST(sum(a.ra_visit_cnt) AS INT64),
       CAST(sum(a.ra_covered_days_visit_cnt) AS INT64),
       CAST(sum(a.ra_non_replaced_blood_unit_qty) AS INT64),
       CAST(sum(a.ra_prescription_qty) AS INT64),
       CAST(ROUND(sum(a.ra_total_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_non_covered_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_net_billed_charge_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_deductible_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_coinsurance_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_net_benefit_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_insurance_payment_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       CAST(ROUND(sum(a.ra_patient_responsible_amt), 3, 'ROUND_HALF_EVEN') AS NUMERIC),
       3 AS schema_id,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM
  (SELECT substr(trim(format('%#34.0f', ragh.id)), length(trim(format('%#34.0f', ragh.id))) - 4, 4) AS remittance_advice_num,
          CASE
              WHEN racp.date_created IS NOT NULL THEN racp.date_created
              ELSE DATE '1800-01-01'
          END AS ra_log_date,
          CASE
              WHEN mpyr.payer_rank IS NOT NULL THEN concat('INS', substr(trim(format('%#4.0f', mpyr.payer_rank)), 1, 1))
              ELSE format('%4d', 0)
          END AS log_id,
          ragh.id AS remittance_header_id,
          racp.id AS remittance_id,
          og.short_name AS unit_num,
          ma.account_no AS ra_pat_acct_num,
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
          coalesce(racp.actual_covered_days, racp.covered_days_visits_count) AS ra_covered_days_num,
          racp.visits AS ra_visit_cnt,
          racp.covered_days_visits_count AS ra_covered_days_visit_cnt,
          CASE
              WHEN upper(rtrim(mpt.ip_op_ind)) = 'O'
                   AND coalesce(racs.raclaimsupplementamt, CAST(0 AS BIGNUMERIC)) <> 0 THEN 'C'
              WHEN upper(rtrim(mpt.ip_op_ind)) = 'I'
                   AND coalesce(racp.pps_operat_outlier_amount, CAST(0 AS NUMERIC)) <> 0
                   OR coalesce(racp.claim_pps_capital_outlier_amnt, CAST(0 AS NUMERIC)) <> 0 THEN 'C'
              ELSE ''
          END AS outlier_ind,
          racp.discharge_fraction AS ra_discharge_fraction_pct,
          racp.reinbursment_rate AS ra_reimbursement_rate_pct,
          racp.not_replaced_blood_units AS ra_non_replaced_blood_unit_qty,
          racp.prescription AS ra_prescription_qty,
          racp.ttl_claim_charge_amount AS ra_total_charge_amt,
          ra_non_covered_charge_amt AS ra_non_covered_charge_amt,
          ra_net_billed_charge_amt AS ra_net_billed_charge_amt,
          coalesce(raaggcat550.raaggcat550amount, CAST(0 AS BIGNUMERIC)) AS ra_deductible_amt,
          coalesce(raaggcat510.raaggcat510amount, CAST(0 AS BIGNUMERIC)) AS ra_coinsurance_amt,
          CASE
              WHEN mpyr.payer_rank = 1 THEN coalesce(racp.ttl_claim_charge_amount, CAST(0 AS NUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)) - coalesce(racp.claim_payment_amount, CAST(0 AS NUMERIC))
              ELSE coalesce(ra_net_billed_charge_amt, CAST(0 AS BIGNUMERIC)) - coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC))
          END AS ra_net_benefit_amt,
          racp.claim_payment_amount AS ra_insurance_payment_amt,
          coalesce(raaggcat500.raaggcat500amount, CAST(0 AS BIGNUMERIC)) AS ra_patient_responsible_amt,
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
          CASE
              WHEN racp.is_deleted = 1 THEN 'N'
              ELSE 'Y'
          END AS active_ind
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_payment AS racp
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat400amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 400 AND 499
      GROUP BY 2,
               3) AS raaggcat400 ON racp.id = raaggcat400.ra_claim_payment_id
   AND racp.schema_id = raaggcat400.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat500amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id BETWEEN 500 AND 599
      GROUP BY 2,
               3) AS raaggcat500 ON racp.id = raaggcat500.ra_claim_payment_id
   AND racp.schema_id = raaggcat500.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat510amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 510
      GROUP BY 2,
               3) AS raaggcat510 ON racp.id = raaggcat510.ra_claim_payment_id
   AND racp.schema_id = raaggcat510.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_835_category_aggregated.amount, CAST(0 AS NUMERIC))) AS raaggcat550amount,
             ra_835_category_aggregated.ra_claim_payment_id,
             ra_835_category_aggregated.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_835_category_aggregated
      WHERE ra_835_category_aggregated.ra_category_id = 550
      GROUP BY 2,
               3) AS raaggcat550 ON racp.id = raaggcat550.ra_claim_payment_id
   AND racp.schema_id = raaggcat550.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(ra_claim_supplement.supplement_amount, CAST(0 AS NUMERIC))) AS raclaimsupplementamt,
             ra_claim_supplement.ra_claim_payment_id,
             ra_claim_supplement.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_supplement
      WHERE upper(rtrim(ra_claim_supplement.supplement_type)) = 'ZM'
      GROUP BY 2,
               3) AS racs ON racp.id = racs.ra_claim_payment_id
   AND racp.schema_id = racs.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_group_header AS ragh ON ragh.id = racp.ra_group_header_id
   AND ragh.schema_id = racp.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account AS ma ON racp.mon_account_id = ma.id
   AND racp.schema_id = ma.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.org AS og ON ma.org_id_provider = og.org_id
   AND ma.schema_id = og.schema_id
   INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_patient_type AS mpt ON mpt.schema_id = ma.schema_id
   AND mpt.id = ma.mon_patient_type_id
   AND mpt.org_id = ma.org_id_provider
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mpyr ON racp.mon_account_payer_id = mpyr.id
   AND racp.schema_id = mpyr.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS pyr ON pyr.id = mpyr.mon_payer_id
   AND pyr.schema_id = mpyr.schema_id
   LEFT OUTER JOIN
     (SELECT max(ra_service.procedure_code) AS procedure_code,
             ra_service.ra_claim_payment_id,
             ra_service.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_service
      WHERE upper(rtrim(ra_service.procedure_code_type)) = 'ZZ'
      GROUP BY 2,
               3,
               upper(ra_service.procedure_code)) AS rasvcode ON racp.id = rasvcode.ra_claim_payment_id
   AND racp.schema_id = rasvcode.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS raci ON raci.ra_claim_payment_id = racp.id
   AND raci.schema_id = racp.schema_id
   AND upper(rtrim(raci.identifier_type)) = '9C'
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.ra_claim_identifier AS racice ON racice.ra_claim_payment_id = racp.id
   AND racice.schema_id = racp.schema_id
   AND upper(rtrim(racice.identifier_type)) = 'CE'
   CROSS JOIN UNNEST(ARRAY[ coalesce(raaggcat400.raaggcat400amount, CAST(0 AS BIGNUMERIC)) ]) AS ra_non_covered_charge_amt
   CROSS JOIN UNNEST(ARRAY[ coalesce(racp.ttl_claim_charge_amount - ra_non_covered_charge_amt, CAST(0 AS BIGNUMERIC)) ]) AS ra_net_billed_charge_amt
   WHERE racp.schema_id = 3
     AND racp.date_created < current_date('US/Central') ) AS a
GROUP BY upper(substr(a.unit_num, 1, 5));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;