DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_eom.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sirisha Seri
      Name: CC_EOM BTEQ Script.
      Mod1: Creation of script on 03/20/13. SW.
	Mod2: Updated query to filter duplicate values by adding qualify in select statement 8/6/2018 PT
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA240;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO  {{ params.param_parallon_ra_core_dataset_name }}.cc_eom AS x USING
  (SELECT keys.patient_dw_id,
          eom.rptg_period,
          keys.company_code,
          eom.coid,
          eom.unit_num,
          eom.pat_acct_num,
          eom.admission_date,
          eom.discharge_date,
          eom.final_bill_date,
          eom.ar_bill_thru_date,
          clmax.calc_date AS eor_log_date,
          NULL AS log84_ind,
          eom.billing_status_code,
          eom.financial_class_code,
          eom.patient_type_code,
          eom.account_status_code,
          eom.total_account_balance_amt,
          eom.total_billed_charges_amt,
          eom.total_payment_amt,
          eom.total_adjustment_amt,
          eom.total_contract_allow_amt,
          coalesce(mapyr.total_expected_payment, CAST(0 AS NUMERIC)) + coalesce(mapyr.state_tax_amt * -1, CAST(0 AS NUMERIC)) + coalesce(svc_gvt.hac_adjmt_amt * -1, CAST(0 AS BIGNUMERIC)) AS eor_gross_reimbursement_amt,
          eor.eor_gross_reimbursement_amt AS prior_day_gross_reimb_amt,
          mapyr.rate_schedule_name AS rate_schedule_name,
          eom.account_id,
          eom.dw_last_update_date_time,
          eom.source_system_code
   FROM  {{ params.param_parallon_ra_stage_dataset_name }}.cc_eom AS eom
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS keys ON upper(rtrim(eom.coid)) = upper(rtrim(keys.coid))
   AND eom.pat_acct_num = keys.pat_acct_num
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapyr ON eom.account_id = mapyr.mon_account_id
   AND eom.schema_id = mapyr.schema_id
   AND mapyr.payer_rank = 1
   LEFT OUTER JOIN
     (SELECT mapcl.mon_account_payer_id,
             mapcl.schema_id,
             mapcl.payer_rank,
             max(mapcl.account_no) AS account_no,
             max(mapcl.id) AS calc_id,
             max(mapcl.calculation_date) AS calc_date,
             sum(coalesce(mapcl.total_charges, CAST(0 AS NUMERIC))) AS total_charges,
             sum(coalesce(mapcl.billed_charges, CAST(0 AS NUMERIC))) AS billed_charges,
             sum(coalesce(mapcl.base_expected_payment, CAST(0 AS NUMERIC))) AS base_expected_payment,
             sum(coalesce(mapcl.exclusion_expected_payment, CAST(0 AS NUMERIC))) AS exclusion_expected_payment,
             sum(coalesce(mapcl.length_of_service, CAST(0 AS NUMERIC))) AS length_of_service,
             sum(coalesce(mapcl.total_expected_payment, CAST(0 AS NUMERIC))) AS total_expected_payment
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl
      WHERE mapcl.is_survivor = 1
      GROUP BY 1,
               2,
               3,
               upper(mapcl.account_no)) AS clmax ON mapyr.id = clmax.mon_account_payer_id
   AND mapyr.schema_id = clmax.schema_id
   AND mapyr.payer_rank = 1
   LEFT OUTER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mpcl ON clmax.calc_id = mpcl.id
   AND clmax.schema_id = mpcl.schema_id
   LEFT OUTER JOIN
     (SELECT sum(coalesce(mapcsvc.gvt_operating_fed_pmt, CAST(0 AS NUMERIC))) AS oper_fed_pmt,
             sum(coalesce(mapcsvc.gvt_capital_cost, CAST(0 AS NUMERIC))) AS gvt_cap_cost, --                     ,Max (Coalesce ( Mapcsvc.Cost_Weight, 0)) As Cost_Wt
 sum(coalesce(mapcsvc.gvt_outlier_payment, CAST(0 AS NUMERIC))) AS gvt_out_pmt,
 sum(coalesce(mapcsvc.expected_payment, CAST(0 AS NUMERIC))) AS exp_pmt,
 sum(coalesce(mapcsvc.gvt_operating_cost, CAST(0 AS NUMERIC))) AS gvt_operating_cost,
 sum(coalesce(mapcsvc.ipf_pps_adj_per_diem * CASE
                                                 WHEN mapcsvc.quantity > 1 THEN mapcsvc.quantity
                                                 ELSE CAST(1 AS NUMERIC)
                                             END, NUMERIC '0')) AS ipf_pmt,
 sum(coalesce(mapcsvc.ipf_pps_out_per_diem_add_on, CAST(0 AS NUMERIC))) AS gvt_outperdiemaddon,
 sum(coalesce(mapcsvc.hac_adjmt_amt, CAST(0 AS NUMERIC))) AS hac_adjmt_amt,
 mapcl.mon_account_payer_id,
 mapcl.schema_id
      FROM  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_service AS mapcsvc
      INNER JOIN  {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcsvc.mon_acct_payer_calc_summary_id = mapcl.id
      AND mapcsvc.schema_id = mapcl.schema_id
      AND mapcl.is_survivor = 1
      GROUP BY 9,
               10) AS svc_gvt ON svc_gvt.mon_account_payer_id = clmax.mon_account_payer_id
   AND svc_gvt.schema_id = clmax.schema_id
   LEFT OUTER JOIN  {{ params.param_parallon_ra_core_dataset_name }}.cc_eor AS eor ON upper(rtrim(eor.coid)) = upper(rtrim(eom.coid))
   AND eor.pat_acct_num = eom.pat_acct_num
   AND eor.iplan_insurance_order_num = 1 QUALIFY row_number() OVER (PARTITION BY keys.patient_dw_id,
                                                                                 upper(eom.rptg_period)
                                                                    ORDER BY eom.pat_acct_num) = 1) AS z ON x.patient_dw_id = z.patient_dw_id
AND upper(rtrim(x.rptg_period)) = upper(rtrim(z.rptg_period)) WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = z.coid,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    admission_date = z.admission_date,
    discharge_date = z.discharge_date,
    final_bill_date = z.final_bill_date,
    ar_bill_thru_date = z.ar_bill_thru_date,
    eor_log_date = DATE(z.eor_log_date),
    log84_ind = CAST(NULL AS STRING),
    billing_status_code = z.billing_status_code,
    financial_class_code = z.financial_class_code,
    patient_type_code = z.patient_type_code,
    account_status_code = z.account_status_code,
    total_account_balance_amt = z.total_account_balance_amt,
    total_billed_charges_amt = z.total_billed_charges_amt,
    total_payment_amt = z.total_payment_amt,
    total_adjustment_amt = z.total_adjustment_amt,
    total_contract_allow_amt = z.total_contract_allow_amt,
    eor_gross_reimbursement_amt = CAST(ROUND(z.eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC),
    prior_day_gross_reimb_amt = z.prior_day_gross_reimb_amt,
    rate_schedule_name = substr(z.rate_schedule_name, 1, 100),
    account_id = z.account_id,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        rptg_period,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        admission_date,
        discharge_date,
        final_bill_date,
        ar_bill_thru_date,
        eor_log_date,
        log84_ind,
        billing_status_code,
        financial_class_code,
        patient_type_code,
        account_status_code,
        total_account_balance_amt,
        total_billed_charges_amt,
        total_payment_amt,
        total_adjustment_amt,
        total_contract_allow_amt,
        eor_gross_reimbursement_amt,
        prior_day_gross_reimb_amt,
        rate_schedule_name,
        account_id,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.rptg_period, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.admission_date, z.discharge_date, z.final_bill_date, z.ar_bill_thru_date, DATE(z.eor_log_date), CAST(NULL AS STRING), z.billing_status_code, z.financial_class_code, z.patient_type_code, z.account_status_code, z.total_account_balance_amt, z.total_billed_charges_amt, z.total_payment_amt, z.total_adjustment_amt, z.total_contract_allow_amt, CAST(ROUND(z.eor_gross_reimbursement_amt, 3, 'ROUND_HALF_EVEN') AS NUMERIC), z.prior_day_gross_reimb_amt, substr(z.rate_schedule_name, 1, 100), z.account_id, datetime_trunc(current_datetime('US/Central'), SECOND), 'N');


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             rptg_period
      FROM  {{ params.param_parallon_ra_core_dataset_name }}.cc_eom
      GROUP BY patient_dw_id,
               rptg_period
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_cur_project_id }}`.ra_edwra.cc_eom');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- - and eor.Financial_Class_COde=EOM.Financial_Class_Code
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE  {{ params.param_parallon_ra_core_dataset_name }}.cc_eom AS eom
SET log84_ind = substr(CASE
                           WHEN (upper(eom.rate_schedule_name) LIKE 'GOV MIPS%'
                                 OR upper(eom.rate_schedule_name) LIKE 'GOV MIRF%'
                                 OR upper(eom.rate_schedule_name) LIKE 'GOV TRICARE IP%')
                                AND (eom.discharge_date IS NULL
                                     OR eom.discharge_date > last_day(date_add(current_date('US/Central'), interval -1 MONTH))
                                     OR eom.final_bill_date IS NULL
                                     OR eom.final_bill_date >= date_add(date_add(date_sub(current_date('US/Central'), interval extract(DAY
                                                                                                                                       FROM current_date('US/Central')) DAY), interval 3 DAY), interval -0 MONTH))
                                AND eom.admission_date < date_add(date_add(date_sub(current_date('US/Central'), interval extract(DAY
                                                                                                                                 FROM current_date('US/Central')) DAY), interval 1 DAY), interval -0 MONTH) THEN 'Y'
                           ELSE 'N'
                       END, 1, 1)
WHERE TRUE;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;