DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_ra_discrepancy_wrk1.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************************************************************
  Developer: Holly Ray
       Date: 2/12/2015. First time collecting metadata.
       Name: CC_RA_Discrepancy_Wrk1.sql
       Mod1: Modified RA_Payment_Date logic to be not null. 2/13/2015. AP.
             Added Join on Iplan_Insurance_Order_Num in the rawrk1. 2/19/2015. AP.
       Mod3: Added QUERY_BAND per Teradata DBA suggestion to meet standards. 11/7/2017 SW.
	Mod4:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
****************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA234_RA_Disc_Wrk1;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE  {{ params.param_parallon_ra_stage_dataset_name }}.ra_discrepancy_wrk1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_stage_dataset_name }}.ra_discrepancy_wrk1 AS mt USING
  (SELECT DISTINCT eorra.company_code AS company_code,
                   eorra.coid AS coid,
                   eorra.patient_dw_id,
                   eorra.payor_dw_id,
                   eorra.iplan_insurance_order_num,
                   eorra.eor_log_date,
                   eorra.log_id AS log_id,
                   eorra.log_sequence_num,
                   eorra.eff_from_date,
                   eorra.unit_num AS unit_num,
                   eorra.pat_acct_num,
                   eorra.iplan_id,
                   eorra.ar_bill_thru_date,
                   cra.remittance_date AS remittance_date,
                   rawrk2.ra_log_date,
                   coalesce(rawrk1.ra_covered_days_num, 0) AS ra_covered_days_num,
                   CAST(ROUND(coalesce(rawrk1.ra_deductible_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_deductible_coinsurance_amt,
                   rawrk4.ra_drg_code AS ra_drg_code,
                   CAST(ROUND(coalesce(rawrk1.ra_insurance_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_payment_amt,
                   cra.payment_date AS ra_payment_date,
                   CAST(ROUND(coalesce(rawrk3.ra_contractual_allowance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_contractual_allowance_amt,
                   rawrk4.ra_hipps_code AS ra_hipps_code,
                   CAST(ROUND(coalesce(rawrk1.ra_total_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_total_charge_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_non_covered_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_non_covered_charge_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_gross_reimbursement_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_gross_reimbursement_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_insurance_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS actual_payment_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_net_billed_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_net_billed_charge_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_deductible_amt, NUMERIC '0') - coalesce(rawrk3.ra_blood_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_deductible_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_total_deductible_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_blood_deductible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_blood_deductible_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_coinsurance_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_coinsurance_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_lab_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_lab_payment_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_therapy_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_therapy_payment_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_primary_payor_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_primary_payor_payment_amt,
                   CAST(ROUND(coalesce(rawrk1.ra_deductible_amt, NUMERIC '0') + coalesce(rawrk1.ra_coinsurance_amt, NUMERIC '0') + coalesce(rawrk3.ra_copay_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_total_patient_resp_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_copay_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_copay_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_outlier_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_outlier_payment_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_capital_payment_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_capital_payment_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_denied_charge_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_denied_charge_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_net_apc_service_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_net_apc_service_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_net_fee_schedule_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_net_fee_schedule_amt,
                   CAST(ROUND(coalesce(rawrk3.ra_patient_responsible_amt, NUMERIC '0'), 3, 'ROUND_HALF_EVEN') AS NUMERIC) AS ra_patient_responsible_amt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS eorra
   INNER JOIN
     (SELECT max(ewrk1.company_code) AS company_code,
             max(ewrk1.coid) AS coid,
             ewrk1.patient_dw_id,
             ewrk1.payor_dw_id,
             ewrk1.iplan_insurance_order_num,
             ewrk1.eor_log_date,
             max(ewrk1.log_id) AS log_id,
             ewrk1.log_sequence_num,
             ewrk1.eff_from_date,
             sum(coalesce(ccr.ra_covered_days_num, 0)) AS ra_covered_days_num,
             sum(coalesce(ccr.ra_covered_days_visit_cnt, 0)) AS ra_covered_days_visit_cnt,
             sum(coalesce(ccr.ra_insurance_payment_amt, NUMERIC '0')) AS ra_insurance_payment_amt,
             sum(coalesce(ccr.ra_deductible_amt, NUMERIC '0') + coalesce(ccr.ra_coinsurance_amt, NUMERIC '0')) AS ra_deductible_coinsurance_amt,
             sum(coalesce(ccr.ra_total_charge_amt, NUMERIC '0')) AS ra_total_charge_amt,
             sum(coalesce(ccr.ra_non_covered_charge_amt, NUMERIC '0')) AS ra_non_covered_charge_amt,
             sum(coalesce(ccr.ra_net_billed_charge_amt, NUMERIC '0')) AS ra_net_billed_charge_amt,
             sum(coalesce(ccr.ra_deductible_amt, NUMERIC '0')) AS ra_deductible_amt,
             sum(coalesce(ccr.ra_coinsurance_amt, NUMERIC '0')) AS ra_coinsurance_amt
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS ewrk1
      INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccr ON upper(rtrim(ewrk1.company_code)) = upper(rtrim(ccr.company_code))
      AND upper(rtrim(ewrk1.coid)) = upper(rtrim(ccr.coid))
      AND ewrk1.patient_dw_id = ccr.patient_dw_id
      AND ewrk1.payor_dw_id = ccr.payor_dw_id
      AND ewrk1.iplan_insurance_order_num = ccr.iplan_insurance_order_num
      WHERE upper(rtrim(ccr.active_ind)) = 'Y'
        AND NOT EXISTS
          (SELECT 1
           FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
           WHERE upper(rtrim(ccr.company_code)) = upper(rtrim(rra.company_code))
             AND upper(rtrim(ccr.coid)) = upper(rtrim(rra.coid))
             AND ccr.patient_dw_id = rra.patient_dw_id
             AND ccr.payor_dw_id = rra.payor_dw_id
             AND ccr.remittance_advice_num = rra.remittance_advice_num
             AND ccr.ra_log_date = rra.ra_log_date
             AND upper(rtrim(ccr.log_id)) = upper(rtrim(rra.log_id))
             AND rra.amount_category_code = 137
             AND rra.reimbursement_amt = ccr.ra_total_charge_amt
             AND ccr.ra_insurance_payment_amt = 0 )
        AND upper(rtrim(CAST(ccr.dw_last_update_date_time AS STRING))) = upper(rtrim(
                                                                                       (SELECT CAST(max(ccrm.dw_last_update_date_time) AS STRING)
                                                                                        FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrm
                                                                                        WHERE ccrm.patient_dw_id = ccr.patient_dw_id )))
      GROUP BY upper(ewrk1.company_code),
               upper(ewrk1.coid),
               3,
               4,
               5,
               6,
               upper(ewrk1.log_id),
               8,
               9
      HAVING sum(coalesce(ccr.ra_covered_days_num, 0)) <> 0
      OR (sum(coalesce(ccr.ra_covered_days_visit_cnt, 0)) <> 0
          OR sum(coalesce(ccr.ra_insurance_payment_amt, NUMERIC '0')) <> 0
          OR sum(coalesce(ccr.ra_total_charge_amt, NUMERIC '0')) <> 0
          OR sum(coalesce(ccr.ra_deductible_amt, NUMERIC '0')) <> 0
          OR sum(coalesce(ccr.ra_coinsurance_amt, NUMERIC '0')) <> 0)) AS rawrk1 ON upper(rtrim(eorra.company_code)) = upper(rtrim(rawrk1.company_code))
   AND upper(rtrim(eorra.coid)) = upper(rtrim(rawrk1.coid))
   AND eorra.patient_dw_id = rawrk1.patient_dw_id
   AND eorra.payor_dw_id = rawrk1.payor_dw_id
   AND eorra.iplan_insurance_order_num = rawrk1.iplan_insurance_order_num
   AND eorra.eor_log_date = rawrk1.eor_log_date
   AND upper(rtrim(eorra.log_id)) = upper(rtrim(rawrk1.log_id))
   AND eorra.log_sequence_num = rawrk1.log_sequence_num
   AND eorra.eff_from_date = rawrk1.eff_from_date
   LEFT OUTER JOIN
     (SELECT max(ewrk1.company_code) AS company_code,
             max(ewrk1.coid) AS coid,
             ewrk1.patient_dw_id,
             ewrk1.payor_dw_id,
             ewrk1.iplan_insurance_order_num,
             ewrk1.eor_log_date,
             max(ewrk1.log_id) AS log_id,
             ewrk1.log_sequence_num,
             ewrk1.eff_from_date,
             ramax.remittance_advice_num,
             ramax.ra_log_date,
             max(ramax.ra_drg_code) AS ra_drg_code,
             max(ramax.ra_hipps_code) AS ra_hipps_code,
             ramax.ra_covered_days_num,
             ramax.remittance_header_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS ewrk1
      INNER JOIN
        (SELECT ccrm.company_code,
                ccrm.coid,
                ccrm.patient_dw_id,
                ccrm.payor_dw_id,
                ccrm.iplan_insurance_order_num,
                ccrm.ra_log_date,
                ccrm.log_id,
                ccrm.log_sequence_num,
                ccrm.remittance_advice_num,
                ccrm.ra_covered_days_num,
                ccrm.ra_drg_code,
                ccrm.ra_hipps_code,
                ccrm.remittance_header_id
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrm
         WHERE ccrm.log_sequence_num =
             (SELECT max(a.log_sequence_num)
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS a
              WHERE upper(rtrim(a.active_ind)) = 'Y'
                AND NOT EXISTS
                  (SELECT 1
                   FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                   WHERE upper(rtrim(a.company_code)) = upper(rtrim(rra.company_code))
                     AND upper(rtrim(a.coid)) = upper(rtrim(rra.coid))
                     AND a.patient_dw_id = rra.patient_dw_id
                     AND a.payor_dw_id = rra.payor_dw_id
                     AND a.remittance_advice_num = rra.remittance_advice_num
                     AND a.ra_log_date = rra.ra_log_date
                     AND upper(rtrim(a.log_id)) = upper(rtrim(rra.log_id))
                     AND rra.amount_category_code = 137
                     AND rra.reimbursement_amt = a.ra_total_charge_amt
                     AND a.ra_insurance_payment_amt = 0 )
                AND upper(rtrim(CAST(ccrm.dw_last_update_date_time AS STRING))) = upper(rtrim(CAST(a.dw_last_update_date_time AS STRING)))
                AND upper(rtrim(ccrm.company_code)) = upper(rtrim(a.company_code))
                AND upper(rtrim(ccrm.coid)) = upper(rtrim(a.coid))
                AND ccrm.patient_dw_id = a.patient_dw_id
                AND ccrm.payor_dw_id = a.payor_dw_id
                AND ccrm.iplan_insurance_order_num = a.iplan_insurance_order_num )
           AND upper(rtrim(ccrm.active_ind)) = 'Y'
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(ccrm.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(ccrm.coid)) = upper(rtrim(rra.coid))
                AND ccrm.patient_dw_id = rra.patient_dw_id
                AND ccrm.payor_dw_id = rra.payor_dw_id
                AND ccrm.remittance_advice_num = rra.remittance_advice_num
                AND ccrm.ra_log_date = rra.ra_log_date
                AND upper(rtrim(ccrm.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = ccrm.ra_total_charge_amt
                AND ccrm.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(CAST(ccrm.dw_last_update_date_time AS STRING))) = upper(rtrim(
                                                                                           (SELECT CAST(max(ccrmi.dw_last_update_date_time) AS STRING)
                                                                                            FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrmi
                                                                                            WHERE ccrm.patient_dw_id = ccrmi.patient_dw_id
                                                                                              AND upper(rtrim(ccrmi.active_ind)) = 'Y'
                                                                                              AND NOT EXISTS
                                                                                                (SELECT 1
                                                                                                 FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                                                                                                 WHERE upper(rtrim(ccrmi.company_code)) = upper(rtrim(rra.company_code))
                                                                                                   AND upper(rtrim(ccrmi.coid)) = upper(rtrim(rra.coid))
                                                                                                   AND ccrmi.patient_dw_id = rra.patient_dw_id
                                                                                                   AND ccrmi.payor_dw_id = rra.payor_dw_id
                                                                                                   AND ccrmi.remittance_advice_num = rra.remittance_advice_num
                                                                                                   AND ccrmi.ra_log_date = rra.ra_log_date
                                                                                                   AND upper(rtrim(ccrmi.log_id)) = upper(rtrim(rra.log_id))
                                                                                                   AND rra.amount_category_code = 137
                                                                                                   AND rra.reimbursement_amt = ccrmi.ra_total_charge_amt
                                                                                                   AND ccrmi.ra_insurance_payment_amt = 0 )
                                                                                              AND ccrm.payor_dw_id = ccrmi.payor_dw_id )))
           AND ccrm.remittance_id =
             (SELECT max(ccrmi.remittance_id)
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrmi
              WHERE ccrm.patient_dw_id = ccrmi.patient_dw_id
                AND upper(rtrim(ccrmi.active_ind)) = 'Y'
                AND upper(rtrim(CAST(ccrm.dw_last_update_date_time AS STRING))) = upper(rtrim(CAST(ccrmi.dw_last_update_date_time AS STRING)))
                AND ccrm.log_sequence_num = ccrmi.log_sequence_num
                AND NOT EXISTS
                  (SELECT 1
                   FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                   WHERE upper(rtrim(ccrmi.company_code)) = upper(rtrim(rra.company_code))
                     AND upper(rtrim(ccrmi.coid)) = upper(rtrim(rra.coid))
                     AND ccrmi.patient_dw_id = rra.patient_dw_id
                     AND ccrmi.payor_dw_id = rra.payor_dw_id
                     AND ccrmi.remittance_advice_num = rra.remittance_advice_num
                     AND ccrmi.ra_log_date = rra.ra_log_date
                     AND upper(rtrim(ccrmi.log_id)) = upper(rtrim(rra.log_id))
                     AND rra.amount_category_code = 137
                     AND rra.reimbursement_amt = ccrmi.ra_total_charge_amt
                     AND ccrmi.ra_insurance_payment_amt = 0 )
                AND ccrm.payor_dw_id = ccrmi.payor_dw_id ) ) AS ramax ON upper(rtrim(ewrk1.company_code)) = upper(rtrim(ramax.company_code))
      AND upper(rtrim(ewrk1.coid)) = upper(rtrim(ramax.coid))
      AND ewrk1.patient_dw_id = ramax.patient_dw_id
      AND ewrk1.payor_dw_id = ramax.payor_dw_id
      AND ewrk1.iplan_insurance_order_num = ramax.iplan_insurance_order_num
      GROUP BY upper(ewrk1.company_code),
               upper(ewrk1.coid),
               3,
               4,
               5,
               6,
               upper(ewrk1.log_id),
               8,
               9,
               10,
               11,
               upper(ramax.ra_drg_code),
               upper(ramax.ra_hipps_code),
               14,
               15) AS rawrk2 ON upper(rtrim(eorra.company_code)) = upper(rtrim(rawrk2.company_code))
   AND upper(rtrim(eorra.coid)) = upper(rtrim(rawrk2.coid))
   AND eorra.patient_dw_id = rawrk2.patient_dw_id
   AND eorra.payor_dw_id = rawrk2.payor_dw_id
   AND eorra.iplan_insurance_order_num = rawrk2.iplan_insurance_order_num
   AND eorra.eor_log_date = rawrk2.eor_log_date
   AND upper(rtrim(eorra.log_id)) = upper(rtrim(rawrk2.log_id))
   AND eorra.log_sequence_num = rawrk2.log_sequence_num
   AND eorra.eff_from_date = rawrk2.eff_from_date
   LEFT OUTER JOIN
     (SELECT erwrk1.company_code,
             erwrk1.coid,
             erwrk1.patient_dw_id,
             erwrk1.payor_dw_id,
             erwrk1.iplan_insurance_order_num,
             erwrk1.eor_log_date,
             erwrk1.log_id,
             erwrk1.log_sequence_num,
             erwrk1.eff_from_date,
             erwrk1.unit_num,
             erwrk1.pat_acct_num,
             erwrk1.iplan_id,
             coalesce(caa.ra_contractual_allowance_amt, NUMERIC '0') AS ra_contractual_allowance_amt,
             coalesce(gra.ra_gross_reimbursement_amt, NUMERIC '0') AS ra_gross_reimbursement_amt,
             coalesce(bda.ra_blood_deductible_amt, NUMERIC '0') AS ra_blood_deductible_amt,
             coalesce(lpa.ra_lab_payment_amt, NUMERIC '0') AS ra_lab_payment_amt,
             coalesce(tpa.ra_therapy_payment_amt, NUMERIC '0') AS ra_therapy_payment_amt,
             coalesce(pppa.ra_primary_payor_payment_amt, NUMERIC '0') AS ra_primary_payor_payment_amt,
             coalesce(ca.ra_copay_amt, NUMERIC '0') AS ra_copay_amt,
             coalesce(opa.ra_outlier_payment_amt, NUMERIC '0') AS ra_outlier_payment_amt,
             coalesce(cpa.ra_capital_payment_amt, NUMERIC '0') AS ra_capital_payment_amt,
             coalesce(dca.ra_denied_charge_amt, NUMERIC '0') AS ra_denied_charge_amt,
             coalesce(nasa.ra_net_apc_service_amt, NUMERIC '0') AS ra_net_apc_service_amt,
             coalesce(nfsa.ra_net_fee_schedule_amt, NUMERIC '0') AS ra_net_fee_schedule_amt,
             coalesce(pra.ra_patient_responsible_amt, NUMERIC '0') AS ra_patient_responsible_amt
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS erwrk1
      LEFT OUTER JOIN
        (SELECT ramt.company_code,
                ramt.coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                coalesce(ramt.reimbursement_amt, NUMERIC '0') AS ra_contractual_allowance_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 138
           AND upper(rtrim(remit.active_ind)) = 'Y'
           AND remit.remittance_id =
             (SELECT max(remit2.remittance_id)
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit2
              WHERE upper(rtrim(remit2.company_code)) = upper(rtrim(remit.company_code))
                AND upper(rtrim(remit2.coid)) = upper(rtrim(remit.coid))
                AND remit2.patient_dw_id = remit.patient_dw_id
                AND remit2.payor_dw_id = remit.payor_dw_id )
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 ) ) AS caa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(caa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(caa.coid))
      AND erwrk1.patient_dw_id = caa.patient_dw_id
      AND erwrk1.payor_dw_id = caa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_gross_reimbursement_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 139
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS gra ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(gra.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(gra.coid))
      AND erwrk1.patient_dw_id = gra.patient_dw_id
      AND erwrk1.payor_dw_id = gra.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_blood_deductible_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 32
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS bda ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(bda.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(bda.coid))
      AND erwrk1.patient_dw_id = bda.patient_dw_id
      AND erwrk1.payor_dw_id = bda.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_lab_payment_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 50
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS lpa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(lpa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(lpa.coid))
      AND erwrk1.patient_dw_id = lpa.patient_dw_id
      AND erwrk1.payor_dw_id = lpa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_therapy_payment_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 93
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS tpa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(tpa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(tpa.coid))
      AND erwrk1.patient_dw_id = tpa.patient_dw_id
      AND erwrk1.payor_dw_id = tpa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_primary_payor_payment_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 133
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS pppa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(pppa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(pppa.coid))
      AND erwrk1.patient_dw_id = pppa.patient_dw_id
      AND erwrk1.payor_dw_id = pppa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_copay_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 123
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS ca ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(ca.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(ca.coid))
      AND erwrk1.patient_dw_id = ca.patient_dw_id
      AND erwrk1.payor_dw_id = ca.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_outlier_payment_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 71
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS opa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(opa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(opa.coid))
      AND erwrk1.patient_dw_id = opa.patient_dw_id
      AND erwrk1.payor_dw_id = opa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_capital_payment_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 8
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS cpa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(cpa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(cpa.coid))
      AND erwrk1.patient_dw_id = cpa.patient_dw_id
      AND erwrk1.payor_dw_id = cpa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_denied_charge_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 137
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS dca ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(dca.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(dca.coid))
      AND erwrk1.patient_dw_id = dca.patient_dw_id
      AND erwrk1.payor_dw_id = dca.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_net_apc_service_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 135
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS nasa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(nasa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(nasa.coid))
      AND erwrk1.patient_dw_id = nasa.patient_dw_id
      AND erwrk1.payor_dw_id = nasa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_net_fee_schedule_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 136
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS nfsa ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(nfsa.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(nfsa.coid))
      AND erwrk1.patient_dw_id = nfsa.patient_dw_id
      AND erwrk1.payor_dw_id = nfsa.payor_dw_id
      LEFT OUTER JOIN
        (SELECT max(ramt.company_code) AS company_code,
                max(ramt.coid) AS coid,
                ramt.patient_dw_id,
                ramt.payor_dw_id,
                sum(coalesce(ramt.reimbursement_amt, NUMERIC '0')) AS ra_patient_responsible_amt
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS ramt
         INNER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS remit ON upper(rtrim(ramt.company_code)) = upper(rtrim(remit.company_code))
         AND upper(rtrim(ramt.coid)) = upper(rtrim(remit.coid))
         AND ramt.patient_dw_id = remit.patient_dw_id
         AND ramt.payor_dw_id = remit.payor_dw_id
         AND ramt.remittance_advice_num = remit.remittance_advice_num
         AND ramt.ra_log_date = remit.ra_log_date
         AND upper(rtrim(ramt.log_id)) = upper(rtrim(remit.log_id))
         AND ramt.log_sequence_num = remit.log_sequence_num
         WHERE ramt.amount_category_code = 119
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(remit.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(remit.coid)) = upper(rtrim(rra.coid))
                AND remit.patient_dw_id = rra.patient_dw_id
                AND remit.payor_dw_id = rra.payor_dw_id
                AND remit.remittance_advice_num = rra.remittance_advice_num
                AND remit.ra_log_date = rra.ra_log_date
                AND upper(rtrim(remit.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = remit.ra_total_charge_amt
                AND remit.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(remit.active_ind)) = 'Y'
         GROUP BY upper(ramt.company_code),
                  upper(ramt.coid),
                  3,
                  4) AS pra ON upper(rtrim(erwrk1.company_code)) = upper(rtrim(pra.company_code))
      AND upper(rtrim(erwrk1.coid)) = upper(rtrim(pra.coid))
      AND erwrk1.patient_dw_id = pra.patient_dw_id
      AND erwrk1.payor_dw_id = pra.payor_dw_id) AS rawrk3 ON upper(rtrim(eorra.company_code)) = upper(rtrim(rawrk3.company_code))
   AND upper(rtrim(eorra.coid)) = upper(rtrim(rawrk3.coid))
   AND eorra.patient_dw_id = rawrk3.patient_dw_id
   AND eorra.payor_dw_id = rawrk3.payor_dw_id
   AND eorra.iplan_insurance_order_num = rawrk3.iplan_insurance_order_num
   AND eorra.eor_log_date = rawrk3.eor_log_date
   AND upper(rtrim(eorra.log_id)) = upper(rtrim(rawrk3.log_id))
   AND eorra.log_sequence_num = rawrk3.log_sequence_num
   AND eorra.eff_from_date = rawrk3.eff_from_date
   LEFT OUTER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance_advice AS cra ON upper(rtrim(cra.company_code)) = upper(rtrim(rawrk2.company_code))
   AND upper(rtrim(cra.coid)) = upper(rtrim(rawrk2.coid))
   AND cra.payor_dw_id = rawrk2.payor_dw_id
   AND cra.remittance_header_id = rawrk2.remittance_header_id
   LEFT OUTER JOIN
     (SELECT max(ewrk1.company_code) AS company_code,
             max(ewrk1.coid) AS coid,
             ewrk1.patient_dw_id,
             ewrk1.payor_dw_id,
             ewrk1.iplan_insurance_order_num,
             ewrk1.eor_log_date,
             max(ewrk1.log_id) AS log_id,
             ewrk1.log_sequence_num,
             ewrk1.eff_from_date,
             max(ramax.ra_drg_code) AS ra_drg_code,
             max(ramax.ra_hipps_code) AS ra_hipps_code
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.eor_discrepancy_wrk1 AS ewrk1
      INNER JOIN
        (SELECT ccrm.company_code,
                ccrm.coid,
                ccrm.patient_dw_id,
                ccrm.payor_dw_id,
                ccrm.iplan_insurance_order_num,
                ccrm.ra_log_date,
                ccrm.log_id,
                ccrm.log_sequence_num,
                ccrm.ra_drg_code,
                ccrm.ra_hipps_code
         FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrm
         WHERE ccrm.log_sequence_num =
             (SELECT max(a.log_sequence_num)
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS a
              WHERE a.ra_drg_code IS NOT NULL
                AND upper(rtrim(a.active_ind)) = 'Y'
                AND NOT EXISTS
                  (SELECT 1
                   FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                   WHERE upper(rtrim(a.company_code)) = upper(rtrim(rra.company_code))
                     AND upper(rtrim(a.coid)) = upper(rtrim(rra.coid))
                     AND a.patient_dw_id = rra.patient_dw_id
                     AND a.payor_dw_id = rra.payor_dw_id
                     AND a.remittance_advice_num = rra.remittance_advice_num
                     AND a.ra_log_date = rra.ra_log_date
                     AND upper(rtrim(a.log_id)) = upper(rtrim(rra.log_id))
                     AND rra.amount_category_code = 137
                     AND rra.reimbursement_amt = a.ra_total_charge_amt
                     AND a.ra_insurance_payment_amt = 0 )
                AND upper(rtrim(ccrm.company_code)) = upper(rtrim(a.company_code))
                AND upper(rtrim(ccrm.coid)) = upper(rtrim(a.coid))
                AND ccrm.patient_dw_id = a.patient_dw_id
                AND ccrm.payor_dw_id = a.payor_dw_id
                AND ccrm.iplan_insurance_order_num = a.iplan_insurance_order_num )
           AND upper(rtrim(ccrm.active_ind)) = 'Y'
           AND NOT EXISTS
             (SELECT 1
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
              WHERE upper(rtrim(ccrm.company_code)) = upper(rtrim(rra.company_code))
                AND upper(rtrim(ccrm.coid)) = upper(rtrim(rra.coid))
                AND ccrm.patient_dw_id = rra.patient_dw_id
                AND ccrm.payor_dw_id = rra.payor_dw_id
                AND ccrm.remittance_advice_num = rra.remittance_advice_num
                AND ccrm.ra_log_date = rra.ra_log_date
                AND upper(rtrim(ccrm.log_id)) = upper(rtrim(rra.log_id))
                AND rra.amount_category_code = 137
                AND rra.reimbursement_amt = ccrm.ra_total_charge_amt
                AND ccrm.ra_insurance_payment_amt = 0 )
           AND upper(rtrim(CAST(ccrm.dw_last_update_date_time AS STRING))) = upper(rtrim(
                                                                                           (SELECT CAST(max(ccrmi.dw_last_update_date_time) AS STRING)
                                                                                            FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrmi
                                                                                            WHERE ccrm.patient_dw_id = ccrmi.patient_dw_id
                                                                                              AND upper(rtrim(ccrmi.active_ind)) = 'Y'
                                                                                              AND NOT EXISTS
                                                                                                (SELECT 1
                                                                                                 FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                                                                                                 WHERE upper(rtrim(ccrmi.company_code)) = upper(rtrim(rra.company_code))
                                                                                                   AND upper(rtrim(ccrmi.coid)) = upper(rtrim(rra.coid))
                                                                                                   AND ccrmi.patient_dw_id = rra.patient_dw_id
                                                                                                   AND ccrmi.payor_dw_id = rra.payor_dw_id
                                                                                                   AND ccrmi.remittance_advice_num = rra.remittance_advice_num
                                                                                                   AND ccrmi.ra_log_date = rra.ra_log_date
                                                                                                   AND upper(rtrim(ccrmi.log_id)) = upper(rtrim(rra.log_id))
                                                                                                   AND rra.amount_category_code = 137
                                                                                                   AND rra.reimbursement_amt = ccrmi.ra_total_charge_amt
                                                                                                   AND ccrmi.ra_insurance_payment_amt = 0 )
                                                                                              AND ccrm.payor_dw_id = ccrmi.payor_dw_id )))
           AND ccrm.remittance_id =
             (SELECT max(ccrmi.remittance_id)
              FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remittance AS ccrmi
              WHERE ccrm.patient_dw_id = ccrmi.patient_dw_id
                AND upper(rtrim(ccrmi.active_ind)) = 'Y'
                AND NOT EXISTS
                  (SELECT 1
                   FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_remit_reimb_amount AS rra
                   WHERE upper(rtrim(ccrmi.company_code)) = upper(rtrim(rra.company_code))
                     AND upper(rtrim(ccrmi.coid)) = upper(rtrim(rra.coid))
                     AND ccrmi.patient_dw_id = rra.patient_dw_id
                     AND ccrmi.payor_dw_id = rra.payor_dw_id
                     AND ccrmi.remittance_advice_num = rra.remittance_advice_num
                     AND ccrmi.ra_log_date = rra.ra_log_date
                     AND upper(rtrim(ccrmi.log_id)) = upper(rtrim(rra.log_id))
                     AND rra.amount_category_code = 137
                     AND rra.reimbursement_amt = ccrmi.ra_total_charge_amt
                     AND ccrmi.ra_insurance_payment_amt = 0 )
                AND ccrm.payor_dw_id = ccrmi.payor_dw_id ) ) AS ramax ON upper(rtrim(ewrk1.company_code)) = upper(rtrim(ramax.company_code))
      AND upper(rtrim(ewrk1.coid)) = upper(rtrim(ramax.coid))
      AND ewrk1.patient_dw_id = ramax.patient_dw_id
      AND ewrk1.payor_dw_id = ramax.payor_dw_id
      AND ewrk1.iplan_insurance_order_num = ramax.iplan_insurance_order_num
      GROUP BY upper(ewrk1.company_code),
               upper(ewrk1.coid),
               3,
               4,
               5,
               6,
               upper(ewrk1.log_id),
               8,
               9,
               upper(ramax.ra_drg_code),
               upper(ramax.ra_hipps_code)) AS rawrk4 ON upper(rtrim(eorra.company_code)) = upper(rtrim(rawrk4.company_code))
   AND upper(rtrim(eorra.coid)) = upper(rtrim(rawrk4.coid))
   AND eorra.patient_dw_id = rawrk4.patient_dw_id
   AND eorra.payor_dw_id = rawrk4.payor_dw_id
   AND eorra.iplan_insurance_order_num = rawrk4.iplan_insurance_order_num
   AND eorra.eor_log_date = rawrk4.eor_log_date
   AND upper(rtrim(eorra.log_id)) = upper(rtrim(rawrk4.log_id))
   AND eorra.log_sequence_num = rawrk4.log_sequence_num
   AND eorra.eff_from_date = rawrk4.eff_from_date) AS ms ON upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1'))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
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
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (coalesce(mt.ar_bill_thru_date, DATE '1970-01-01') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-01')
     AND coalesce(mt.ar_bill_thru_date, DATE '1970-01-02') = coalesce(ms.ar_bill_thru_date, DATE '1970-01-02'))
AND (coalesce(mt.remittance_date, DATE '1970-01-01') = coalesce(ms.remittance_date, DATE '1970-01-01')
     AND coalesce(mt.remittance_date, DATE '1970-01-02') = coalesce(ms.remittance_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_log_date, DATE '1970-01-01') = coalesce(ms.ra_log_date, DATE '1970-01-01')
     AND coalesce(mt.ra_log_date, DATE '1970-01-02') = coalesce(ms.ra_log_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_covered_days_num, 0) = coalesce(ms.ra_covered_days_num, 0)
     AND coalesce(mt.ra_covered_days_num, 1) = coalesce(ms.ra_covered_days_num, 1))
AND (coalesce(mt.ra_deductible_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_deductible_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_deductible_coinsurance_amt, NUMERIC '1'))
AND (upper(coalesce(mt.ra_drg_code, '0')) = upper(coalesce(ms.ra_drg_code, '0'))
     AND upper(coalesce(mt.ra_drg_code, '1')) = upper(coalesce(ms.ra_drg_code, '1')))
AND (coalesce(mt.ra_payment_amt, NUMERIC '0') = coalesce(ms.ra_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_payment_amt, NUMERIC '1') = coalesce(ms.ra_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_payment_date, DATE '1970-01-01') = coalesce(ms.ra_payment_date, DATE '1970-01-01')
     AND coalesce(mt.ra_payment_date, DATE '1970-01-02') = coalesce(ms.ra_payment_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_contractual_allowance_amt, NUMERIC '0') = coalesce(ms.ra_contractual_allowance_amt, NUMERIC '0')
     AND coalesce(mt.ra_contractual_allowance_amt, NUMERIC '1') = coalesce(ms.ra_contractual_allowance_amt, NUMERIC '1'))
AND (upper(coalesce(mt.ra_hipps_code, '0')) = upper(coalesce(ms.ra_hipps_code, '0'))
     AND upper(coalesce(mt.ra_hipps_code, '1')) = upper(coalesce(ms.ra_hipps_code, '1')))
AND (coalesce(mt.ra_total_charge_amt, NUMERIC '0') = coalesce(ms.ra_total_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_charge_amt, NUMERIC '1') = coalesce(ms.ra_total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_non_covered_charge_amt, NUMERIC '0') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_non_covered_charge_amt, NUMERIC '1') = coalesce(ms.ra_non_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_gross_reimbursement_amt, NUMERIC '0') = coalesce(ms.ra_gross_reimbursement_amt, NUMERIC '0')
     AND coalesce(mt.ra_gross_reimbursement_amt, NUMERIC '1') = coalesce(ms.ra_gross_reimbursement_amt, NUMERIC '1'))
AND (coalesce(mt.actual_payment_amt, NUMERIC '0') = coalesce(ms.actual_payment_amt, NUMERIC '0')
     AND coalesce(mt.actual_payment_amt, NUMERIC '1') = coalesce(ms.actual_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_billed_charge_amt, NUMERIC '0') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_billed_charge_amt, NUMERIC '1') = coalesce(ms.ra_net_billed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_deductible_amt, NUMERIC '0') = coalesce(ms.ra_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_amt, NUMERIC '1') = coalesce(ms.ra_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_total_deductible_amt, NUMERIC '0') = coalesce(ms.ra_total_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_deductible_amt, NUMERIC '1') = coalesce(ms.ra_total_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_blood_deductible_amt, NUMERIC '0') = coalesce(ms.ra_blood_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_blood_deductible_amt, NUMERIC '1') = coalesce(ms.ra_blood_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_lab_payment_amt, NUMERIC '0') = coalesce(ms.ra_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_lab_payment_amt, NUMERIC '1') = coalesce(ms.ra_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_therapy_payment_amt, NUMERIC '0') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_therapy_payment_amt, NUMERIC '1') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_primary_payor_payment_amt, NUMERIC '0') = coalesce(ms.ra_primary_payor_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_primary_payor_payment_amt, NUMERIC '1') = coalesce(ms.ra_primary_payor_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_total_patient_resp_amt, NUMERIC '0') = coalesce(ms.ra_total_patient_resp_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_patient_resp_amt, NUMERIC '1') = coalesce(ms.ra_total_patient_resp_amt, NUMERIC '1'))
AND (coalesce(mt.ra_copay_amt, NUMERIC '0') = coalesce(ms.ra_copay_amt, NUMERIC '0')
     AND coalesce(mt.ra_copay_amt, NUMERIC '1') = coalesce(ms.ra_copay_amt, NUMERIC '1'))
AND (coalesce(mt.ra_outlier_payment_amt, NUMERIC '0') = coalesce(ms.ra_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_outlier_payment_amt, NUMERIC '1') = coalesce(ms.ra_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_capital_payment_amt, NUMERIC '0') = coalesce(ms.ra_capital_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_capital_payment_amt, NUMERIC '1') = coalesce(ms.ra_capital_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_denied_charge_amt, NUMERIC '0') = coalesce(ms.ra_denied_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_denied_charge_amt, NUMERIC '1') = coalesce(ms.ra_denied_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_apc_service_amt, NUMERIC '0') = coalesce(ms.ra_net_apc_service_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_apc_service_amt, NUMERIC '1') = coalesce(ms.ra_net_apc_service_amt, NUMERIC '1'))
AND (coalesce(mt.ra_net_fee_schedule_amt, NUMERIC '0') = coalesce(ms.ra_net_fee_schedule_amt, NUMERIC '0')
     AND coalesce(mt.ra_net_fee_schedule_amt, NUMERIC '1') = coalesce(ms.ra_net_fee_schedule_amt, NUMERIC '1'))
AND (coalesce(mt.ra_patient_responsible_amt, NUMERIC '0') = coalesce(ms.ra_patient_responsible_amt, NUMERIC '0')
     AND coalesce(mt.ra_patient_responsible_amt, NUMERIC '1') = coalesce(ms.ra_patient_responsible_amt, NUMERIC '1'))
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
        unit_num,
        pat_acct_num,
        iplan_id,
        ar_bill_thru_date,
        remittance_date,
        ra_log_date,
        ra_covered_days_num,
        ra_deductible_coinsurance_amt,
        ra_drg_code,
        ra_payment_amt,
        ra_payment_date,
        ra_contractual_allowance_amt,
        ra_hipps_code,
        ra_total_charge_amt,
        ra_non_covered_charge_amt,
        ra_gross_reimbursement_amt,
        actual_payment_amt,
        ra_net_billed_charge_amt,
        ra_deductible_amt,
        ra_total_deductible_amt,
        ra_blood_deductible_amt,
        ra_coinsurance_amt,
        ra_lab_payment_amt,
        ra_therapy_payment_amt,
        ra_primary_payor_payment_amt,
        ra_total_patient_resp_amt,
        ra_copay_amt,
        ra_outlier_payment_amt,
        ra_capital_payment_amt,
        ra_denied_charge_amt,
        ra_net_apc_service_amt,
        ra_net_fee_schedule_amt,
        ra_patient_responsible_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.company_code, ms.coid, ms.patient_dw_id, ms.payor_dw_id, ms.iplan_insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.unit_num, ms.pat_acct_num, ms.iplan_id, ms.ar_bill_thru_date, ms.remittance_date, ms.ra_log_date, ms.ra_covered_days_num, ms.ra_deductible_coinsurance_amt, ms.ra_drg_code, ms.ra_payment_amt, ms.ra_payment_date, ms.ra_contractual_allowance_amt, ms.ra_hipps_code, ms.ra_total_charge_amt, ms.ra_non_covered_charge_amt, ms.ra_gross_reimbursement_amt, ms.actual_payment_amt, ms.ra_net_billed_charge_amt, ms.ra_deductible_amt, ms.ra_total_deductible_amt, ms.ra_blood_deductible_amt, ms.ra_coinsurance_amt, ms.ra_lab_payment_amt, ms.ra_therapy_payment_amt, ms.ra_primary_payor_payment_amt, ms.ra_total_patient_resp_amt, ms.ra_copay_amt, ms.ra_outlier_payment_amt, ms.ra_capital_payment_amt, ms.ra_denied_charge_amt, ms.ra_net_apc_service_amt, ms.ra_net_fee_schedule_amt, ms.ra_patient_responsible_amt, ms.dw_last_update_date_time, ms.source_system_code);


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
             eff_from_date
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.ra_discrepancy_wrk1
      GROUP BY company_code,
               coid,
               patient_dw_id,
               payor_dw_id,
               iplan_insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_stage_dataset_name }}`.ra_discrepancy_wrk1');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('ra_edwra_STAGING','RA_Discrepancy_Wrk1');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

RETURN;

RETURN;