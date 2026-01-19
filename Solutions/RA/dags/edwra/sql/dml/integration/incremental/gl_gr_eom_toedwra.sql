DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/gl_gr_eom_tora_edwra.sql
-- Translated from: bteq
-- Translated to: BigQuery
DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

-- bteq << EOF > /etl/ST/MC/CC_EDW/TgtFiles/GL_GR_EOM_ToEDWRA.out;
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_core_dataset_name }}.cc_gr_eom;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_gr_eom AS mt USING
  (SELECT DISTINCT cak.patient_dw_id,
                   grec.report_name,
                   grec.period_end_date,
                   grec.report_run_date_time,
                   grec.section_name,
                   grec.reporting_period,
                   substr(coalesce(ros.company_code, 'H'), 1, 1) AS company_code,
                   grec.coid AS coid,
                   grec.facility_name,
                   grec.unit_num AS unit_num,
                   grec.pat_acct_num,
                   grec.patient_name,
                   grec.rate_schedule_name,
                   grec.section_month,
                   grec.cost_report_year AS cost_report_year,
                   grec.account_status_code AS account_status_code,
                   grec.auto_posted_flag AS auto_posted_flag,
                   grec.cmg_code AS cmg_code,
                   grec.drg_code AS drg_code,
                   grec.ra_drg_code AS ra_drg_code,
                   ROUND(grec.cmg_weight_amt, 4, 'ROUND_HALF_EVEN') AS cmg_weight_amt,
                   ROUND(grec.drg_weight_amt, 4, 'ROUND_HALF_EVEN') AS drg_weight_amt,
                   ROUND(grec.prorated_drg_weight_amt, 4, 'ROUND_HALF_EVEN') AS prorated_drg_weight_amt,
                   grec.exp_hipps_cmg_code AS exp_hipps_cmg_code,
                   grec.ra_hipps_cmg_code AS ra_hipps_cmg_code,
                   grec.var_drg_flag AS var_drg_flag,
                   grec.var_cmg_flag AS var_cmg_flag,
                   grec.discharge_admit_date,
                   grec.claim_submit_date,
                   grec.ra_payment_date,
                   grec.covered_days_los_cnt,
                   grec.days_billed_cnt,
                   grec.discharge_days_cnt,
                   grec.ra_covered_days_cnt,
                   grec.remit_cnt,
                   grec.var_covered_days_cnt,
                   grec.account_balance_amt,
                   grec.accrued_capital_subtotal_amt,
                   grec.accrued_operating_subtotal_amt,
                   grec.accrued_payment_amt,
                   grec.apc_charges_amt,
                   grec.apc_payment_amt,
                   grec.drg_arithmetic_los_amt,
                   grec.capital_payment_subtotal_amt,
                   grec.cmac_charge_amt,
                   grec.cmac_payment_amt,
                   grec.cmg_payment_amt,
                   grec.coinsurance_deductible_amt,
                   grec.computed_reimb_amt,
                   grec.cost_outlier_payment_amt,
                   grec.covered_charges_amt,
                   grec.drg_payment_amt,
                   grec.ect_payment_amt,
                   grec.education_payment_amt,
                   grec.fee_based_payment_amt,
                   grec.idme_payment_amt,
                   grec.lab_charge_amt,
                   grec.lab_payment_amt,
                   grec.lip_payment_amt,
                   grec.non_covered_charges_amt,
                   grec.operating_drg_payment_amt,
                   grec.operating_dsh_amt,
                   grec.operating_outlier_amt,
                   grec.operating_cost_outlier_amt,
                   grec.other_service_charge_amt,
                   grec.other_service_payment_amt,
                   grec.outlier_payment_amt,
                   grec.payment_adj_amt,
                   grec.ra_coinsurance_amt,
                   grec.ra_contractual_amt,
                   grec.ra_covered_charge_amt,
                   grec.ra_deductible_amt,
                   grec.ra_drg_amt,
                   grec.ra_idme_amt,
                   grec.ra_lab_payment_amt,
                   grec.ra_operating_dsh_amt,
                   grec.ra_operating_idme_amt,
                   grec.ra_operating_outlier_amt,
                   grec.ra_outlier_amt,
                   grec.ra_therapy_payment_amt,
                   grec.ra_total_capital_amt,
                   grec.ra_total_payment_amt,
                   grec.replace_device_reduction_amt,
                   grec.rug_payment_amt,
                   grec.sequestration_amt,
                   grec.short_stay_outlier_payment_amt,
                   grec.tech_add_on_amt,
                   grec.tech_add_on_payment_amt,
                   grec.therapy_charge_amt,
                   grec.therapy_payment_amt,
                   grec.total_charge_amt,
                   grec.total_expected_payment_amt,
                   grec.total_opr_cptl_payment_amt,
                   grec.total_payment_amt,
                   grec.transfer_payment_amt,
                   grec.var_coinsurance_amt,
                   grec.var_contractual_amt,
                   grec.var_covered_charges_amt,
                   grec.var_deductible_amt,
                   grec.var_imed_educ_amt,
                   grec.var_lab_payment_amt,
                   grec.var_operating_dsh_amt,
                   grec.var_operating_ime_amt,
                   grec.var_operating_outlier_amt,
                   grec.var_outlier_amt,
                   grec.var_therapy_payment_amt,
                   grec.var_total_capital_amt,
                   grec.var_total_payment_amt,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   ros.source_system_code AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_gr_eom AS grec
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS ros ON upper(rtrim(grec.coid)) = upper(rtrim(ros.coid))
   INNER JOIN {{ params.param_auth_base_views_dataset_name }}.clinical_acctkeys AS cak ON upper(rtrim(grec.coid)) = upper(rtrim(cak.coid))
   AND grec.pat_acct_num = cak.pat_acct_num
   AND upper(rtrim(cak.company_code)) = upper(rtrim(ros.company_code))) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (upper(coalesce(mt.report_name, '0')) = upper(coalesce(ms.report_name, '0'))
     AND upper(coalesce(mt.report_name, '1')) = upper(coalesce(ms.report_name, '1')))
AND (coalesce(mt.period_end_date, DATE '1970-01-01') = coalesce(ms.period_end_date, DATE '1970-01-01')
     AND coalesce(mt.period_end_date, DATE '1970-01-02') = coalesce(ms.period_end_date, DATE '1970-01-02'))
AND (coalesce(mt.report_run_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.report_run_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.report_run_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.report_run_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.reporting_period, '0')) = upper(coalesce(ms.reporting_period, '0'))
     AND upper(coalesce(mt.reporting_period, '1')) = upper(coalesce(ms.reporting_period, '1')))
AND (upper(coalesce(mt.section_name, '0')) = upper(coalesce(ms.section_name, '0'))
     AND upper(coalesce(mt.section_name, '1')) = upper(coalesce(ms.section_name, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (upper(coalesce(mt.facility_name, '0')) = upper(coalesce(ms.facility_name, '0'))
     AND upper(coalesce(mt.facility_name, '1')) = upper(coalesce(ms.facility_name, '1')))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (upper(coalesce(mt.patient_name, '0')) = upper(coalesce(ms.patient_name, '0'))
     AND upper(coalesce(mt.patient_name, '1')) = upper(coalesce(ms.patient_name, '1')))
AND (upper(coalesce(mt.rate_schedule_name, '0')) = upper(coalesce(ms.rate_schedule_name, '0'))
     AND upper(coalesce(mt.rate_schedule_name, '1')) = upper(coalesce(ms.rate_schedule_name, '1')))
AND (upper(coalesce(mt.section_month, '0')) = upper(coalesce(ms.section_month, '0'))
     AND upper(coalesce(mt.section_month, '1')) = upper(coalesce(ms.section_month, '1')))
AND (upper(coalesce(mt.cost_report_year, '0')) = upper(coalesce(ms.cost_report_year, '0'))
     AND upper(coalesce(mt.cost_report_year, '1')) = upper(coalesce(ms.cost_report_year, '1')))
AND (upper(coalesce(mt.account_status_code, '0')) = upper(coalesce(ms.account_status_code, '0'))
     AND upper(coalesce(mt.account_status_code, '1')) = upper(coalesce(ms.account_status_code, '1')))
AND (upper(coalesce(mt.auto_posted_flag, '0')) = upper(coalesce(ms.auto_posted_flag, '0'))
     AND upper(coalesce(mt.auto_posted_flag, '1')) = upper(coalesce(ms.auto_posted_flag, '1')))
AND (upper(coalesce(mt.cmg_code, '0')) = upper(coalesce(ms.cmg_code, '0'))
     AND upper(coalesce(mt.cmg_code, '1')) = upper(coalesce(ms.cmg_code, '1')))
AND (upper(coalesce(mt.drg_code, '0')) = upper(coalesce(ms.drg_code, '0'))
     AND upper(coalesce(mt.drg_code, '1')) = upper(coalesce(ms.drg_code, '1')))
AND (upper(coalesce(mt.ra_drg_code, '0')) = upper(coalesce(ms.ra_drg_code, '0'))
     AND upper(coalesce(mt.ra_drg_code, '1')) = upper(coalesce(ms.ra_drg_code, '1')))
AND (coalesce(mt.cmg_weight_amt, NUMERIC '0') = coalesce(ms.cmg_weight_amt, NUMERIC '0')
     AND coalesce(mt.cmg_weight_amt, NUMERIC '1') = coalesce(ms.cmg_weight_amt, NUMERIC '1'))
AND (coalesce(mt.drg_weight_amt, NUMERIC '0') = coalesce(ms.drg_weight_amt, NUMERIC '0')
     AND coalesce(mt.drg_weight_amt, NUMERIC '1') = coalesce(ms.drg_weight_amt, NUMERIC '1'))
AND (coalesce(mt.prorated_drg_weight_amt, NUMERIC '0') = coalesce(ms.prorated_drg_weight_amt, NUMERIC '0')
     AND coalesce(mt.prorated_drg_weight_amt, NUMERIC '1') = coalesce(ms.prorated_drg_weight_amt, NUMERIC '1'))
AND (upper(coalesce(mt.exp_hipps_cmg_code, '0')) = upper(coalesce(ms.exp_hipps_cmg_code, '0'))
     AND upper(coalesce(mt.exp_hipps_cmg_code, '1')) = upper(coalesce(ms.exp_hipps_cmg_code, '1')))
AND (upper(coalesce(mt.ra_hipps_cmg_code, '0')) = upper(coalesce(ms.ra_hipps_cmg_code, '0'))
     AND upper(coalesce(mt.ra_hipps_cmg_code, '1')) = upper(coalesce(ms.ra_hipps_cmg_code, '1')))
AND (upper(coalesce(mt.var_drg_flag, '0')) = upper(coalesce(ms.var_drg_flag, '0'))
     AND upper(coalesce(mt.var_drg_flag, '1')) = upper(coalesce(ms.var_drg_flag, '1')))
AND (upper(coalesce(mt.var_cmg_flag, '0')) = upper(coalesce(ms.var_cmg_flag, '0'))
     AND upper(coalesce(mt.var_cmg_flag, '1')) = upper(coalesce(ms.var_cmg_flag, '1')))
AND (coalesce(mt.discharge_admit_date, DATE '1970-01-01') = coalesce(ms.discharge_admit_date, DATE '1970-01-01')
     AND coalesce(mt.discharge_admit_date, DATE '1970-01-02') = coalesce(ms.discharge_admit_date, DATE '1970-01-02'))
AND (coalesce(mt.claim_submit_date, DATE '1970-01-01') = coalesce(ms.claim_submit_date, DATE '1970-01-01')
     AND coalesce(mt.claim_submit_date, DATE '1970-01-02') = coalesce(ms.claim_submit_date, DATE '1970-01-02'))
AND (coalesce(mt.ra_payment_date, DATE '1970-01-01') = coalesce(ms.ra_payment_date, DATE '1970-01-01')
     AND coalesce(mt.ra_payment_date, DATE '1970-01-02') = coalesce(ms.ra_payment_date, DATE '1970-01-02'))
AND (coalesce(mt.covered_days_los_cnt, 0) = coalesce(ms.covered_days_los_cnt, 0)
     AND coalesce(mt.covered_days_los_cnt, 1) = coalesce(ms.covered_days_los_cnt, 1))
AND (coalesce(mt.days_billed_cnt, 0) = coalesce(ms.days_billed_cnt, 0)
     AND coalesce(mt.days_billed_cnt, 1) = coalesce(ms.days_billed_cnt, 1))
AND (coalesce(mt.discharge_days_cnt, 0) = coalesce(ms.discharge_days_cnt, 0)
     AND coalesce(mt.discharge_days_cnt, 1) = coalesce(ms.discharge_days_cnt, 1))
AND (coalesce(mt.ra_covered_days_cnt, 0) = coalesce(ms.ra_covered_days_cnt, 0)
     AND coalesce(mt.ra_covered_days_cnt, 1) = coalesce(ms.ra_covered_days_cnt, 1))
AND (coalesce(mt.remit_cnt, 0) = coalesce(ms.remit_cnt, 0)
     AND coalesce(mt.remit_cnt, 1) = coalesce(ms.remit_cnt, 1))
AND (coalesce(mt.var_covered_days_cnt, 0) = coalesce(ms.var_covered_days_cnt, 0)
     AND coalesce(mt.var_covered_days_cnt, 1) = coalesce(ms.var_covered_days_cnt, 1))
AND (coalesce(mt.account_balance_amt, NUMERIC '0') = coalesce(ms.account_balance_amt, NUMERIC '0')
     AND coalesce(mt.account_balance_amt, NUMERIC '1') = coalesce(ms.account_balance_amt, NUMERIC '1'))
AND (coalesce(mt.accrued_capital_subtotal_amt, NUMERIC '0') = coalesce(ms.accrued_capital_subtotal_amt, NUMERIC '0')
     AND coalesce(mt.accrued_capital_subtotal_amt, NUMERIC '1') = coalesce(ms.accrued_capital_subtotal_amt, NUMERIC '1'))
AND (coalesce(mt.accrued_operating_subtotal_amt, NUMERIC '0') = coalesce(ms.accrued_operating_subtotal_amt, NUMERIC '0')
     AND coalesce(mt.accrued_operating_subtotal_amt, NUMERIC '1') = coalesce(ms.accrued_operating_subtotal_amt, NUMERIC '1'))
AND (coalesce(mt.accrued_payment_amt, NUMERIC '0') = coalesce(ms.accrued_payment_amt, NUMERIC '0')
     AND coalesce(mt.accrued_payment_amt, NUMERIC '1') = coalesce(ms.accrued_payment_amt, NUMERIC '1'))
AND (coalesce(mt.apc_charges_amt, NUMERIC '0') = coalesce(ms.apc_charges_amt, NUMERIC '0')
     AND coalesce(mt.apc_charges_amt, NUMERIC '1') = coalesce(ms.apc_charges_amt, NUMERIC '1'))
AND (coalesce(mt.apc_payment_amt, NUMERIC '0') = coalesce(ms.apc_payment_amt, NUMERIC '0')
     AND coalesce(mt.apc_payment_amt, NUMERIC '1') = coalesce(ms.apc_payment_amt, NUMERIC '1'))
AND (coalesce(mt.drg_arithmetic_los_amt, NUMERIC '0') = coalesce(ms.drg_arithmetic_los_amt, NUMERIC '0')
     AND coalesce(mt.drg_arithmetic_los_amt, NUMERIC '1') = coalesce(ms.drg_arithmetic_los_amt, NUMERIC '1'))
AND (coalesce(mt.capital_payment_subtotal_amt, NUMERIC '0') = coalesce(ms.capital_payment_subtotal_amt, NUMERIC '0')
     AND coalesce(mt.capital_payment_subtotal_amt, NUMERIC '1') = coalesce(ms.capital_payment_subtotal_amt, NUMERIC '1'))
AND (coalesce(mt.cmac_charge_amt, NUMERIC '0') = coalesce(ms.cmac_charge_amt, NUMERIC '0')
     AND coalesce(mt.cmac_charge_amt, NUMERIC '1') = coalesce(ms.cmac_charge_amt, NUMERIC '1'))
AND (coalesce(mt.cmac_payment_amt, NUMERIC '0') = coalesce(ms.cmac_payment_amt, NUMERIC '0')
     AND coalesce(mt.cmac_payment_amt, NUMERIC '1') = coalesce(ms.cmac_payment_amt, NUMERIC '1'))
AND (coalesce(mt.cmg_payment_amt, NUMERIC '0') = coalesce(ms.cmg_payment_amt, NUMERIC '0')
     AND coalesce(mt.cmg_payment_amt, NUMERIC '1') = coalesce(ms.cmg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.coinsurance_deductible_amt, NUMERIC '0') = coalesce(ms.coinsurance_deductible_amt, NUMERIC '0')
     AND coalesce(mt.coinsurance_deductible_amt, NUMERIC '1') = coalesce(ms.coinsurance_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.computed_reimb_amt, NUMERIC '0') = coalesce(ms.computed_reimb_amt, NUMERIC '0')
     AND coalesce(mt.computed_reimb_amt, NUMERIC '1') = coalesce(ms.computed_reimb_amt, NUMERIC '1'))
AND (coalesce(mt.cost_outlier_payment_amt, NUMERIC '0') = coalesce(ms.cost_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.cost_outlier_payment_amt, NUMERIC '1') = coalesce(ms.cost_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.covered_charges_amt, NUMERIC '0') = coalesce(ms.covered_charges_amt, NUMERIC '0')
     AND coalesce(mt.covered_charges_amt, NUMERIC '1') = coalesce(ms.covered_charges_amt, NUMERIC '1'))
AND (coalesce(mt.drg_payment_amt, NUMERIC '0') = coalesce(ms.drg_payment_amt, NUMERIC '0')
     AND coalesce(mt.drg_payment_amt, NUMERIC '1') = coalesce(ms.drg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ect_payment_amt, NUMERIC '0') = coalesce(ms.ect_payment_amt, NUMERIC '0')
     AND coalesce(mt.ect_payment_amt, NUMERIC '1') = coalesce(ms.ect_payment_amt, NUMERIC '1'))
AND (coalesce(mt.education_payment_amt, NUMERIC '0') = coalesce(ms.education_payment_amt, NUMERIC '0')
     AND coalesce(mt.education_payment_amt, NUMERIC '1') = coalesce(ms.education_payment_amt, NUMERIC '1'))
AND (coalesce(mt.fee_based_payment_amt, NUMERIC '0') = coalesce(ms.fee_based_payment_amt, NUMERIC '0')
     AND coalesce(mt.fee_based_payment_amt, NUMERIC '1') = coalesce(ms.fee_based_payment_amt, NUMERIC '1'))
AND (coalesce(mt.idme_payment_amt, NUMERIC '0') = coalesce(ms.idme_payment_amt, NUMERIC '0')
     AND coalesce(mt.idme_payment_amt, NUMERIC '1') = coalesce(ms.idme_payment_amt, NUMERIC '1'))
AND (coalesce(mt.lab_charge_amt, NUMERIC '0') = coalesce(ms.lab_charge_amt, NUMERIC '0')
     AND coalesce(mt.lab_charge_amt, NUMERIC '1') = coalesce(ms.lab_charge_amt, NUMERIC '1'))
AND (coalesce(mt.lab_payment_amt, NUMERIC '0') = coalesce(ms.lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.lab_payment_amt, NUMERIC '1') = coalesce(ms.lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.lip_payment_amt, NUMERIC '0') = coalesce(ms.lip_payment_amt, NUMERIC '0')
     AND coalesce(mt.lip_payment_amt, NUMERIC '1') = coalesce(ms.lip_payment_amt, NUMERIC '1'))
AND (coalesce(mt.non_covered_charges_amt, NUMERIC '0') = coalesce(ms.non_covered_charges_amt, NUMERIC '0')
     AND coalesce(mt.non_covered_charges_amt, NUMERIC '1') = coalesce(ms.non_covered_charges_amt, NUMERIC '1'))
AND (coalesce(mt.operating_drg_payment_amt, NUMERIC '0') = coalesce(ms.operating_drg_payment_amt, NUMERIC '0')
     AND coalesce(mt.operating_drg_payment_amt, NUMERIC '1') = coalesce(ms.operating_drg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.operating_dsh_amt, NUMERIC '0') = coalesce(ms.operating_dsh_amt, NUMERIC '0')
     AND coalesce(mt.operating_dsh_amt, NUMERIC '1') = coalesce(ms.operating_dsh_amt, NUMERIC '1'))
AND (coalesce(mt.operating_outlier_amt, NUMERIC '0') = coalesce(ms.operating_outlier_amt, NUMERIC '0')
     AND coalesce(mt.operating_outlier_amt, NUMERIC '1') = coalesce(ms.operating_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.operating_cost_outlier_amt, NUMERIC '0') = coalesce(ms.operating_cost_outlier_amt, NUMERIC '0')
     AND coalesce(mt.operating_cost_outlier_amt, NUMERIC '1') = coalesce(ms.operating_cost_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.other_service_charge_amt, NUMERIC '0') = coalesce(ms.other_service_charge_amt, NUMERIC '0')
     AND coalesce(mt.other_service_charge_amt, NUMERIC '1') = coalesce(ms.other_service_charge_amt, NUMERIC '1'))
AND (coalesce(mt.other_service_payment_amt, NUMERIC '0') = coalesce(ms.other_service_payment_amt, NUMERIC '0')
     AND coalesce(mt.other_service_payment_amt, NUMERIC '1') = coalesce(ms.other_service_payment_amt, NUMERIC '1'))
AND (coalesce(mt.outlier_payment_amt, NUMERIC '0') = coalesce(ms.outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.outlier_payment_amt, NUMERIC '1') = coalesce(ms.outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.payment_adj_amt, NUMERIC '0') = coalesce(ms.payment_adj_amt, NUMERIC '0')
     AND coalesce(mt.payment_adj_amt, NUMERIC '1') = coalesce(ms.payment_adj_amt, NUMERIC '1'))
AND (coalesce(mt.ra_coinsurance_amt, NUMERIC '0') = coalesce(ms.ra_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.ra_coinsurance_amt, NUMERIC '1') = coalesce(ms.ra_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.ra_contractual_amt, NUMERIC '0') = coalesce(ms.ra_contractual_amt, NUMERIC '0')
     AND coalesce(mt.ra_contractual_amt, NUMERIC '1') = coalesce(ms.ra_contractual_amt, NUMERIC '1'))
AND (coalesce(mt.ra_covered_charge_amt, NUMERIC '0') = coalesce(ms.ra_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.ra_covered_charge_amt, NUMERIC '1') = coalesce(ms.ra_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.ra_deductible_amt, NUMERIC '0') = coalesce(ms.ra_deductible_amt, NUMERIC '0')
     AND coalesce(mt.ra_deductible_amt, NUMERIC '1') = coalesce(ms.ra_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.ra_drg_amt, NUMERIC '0') = coalesce(ms.ra_drg_amt, NUMERIC '0')
     AND coalesce(mt.ra_drg_amt, NUMERIC '1') = coalesce(ms.ra_drg_amt, NUMERIC '1'))
AND (coalesce(mt.ra_idme_amt, NUMERIC '0') = coalesce(ms.ra_idme_amt, NUMERIC '0')
     AND coalesce(mt.ra_idme_amt, NUMERIC '1') = coalesce(ms.ra_idme_amt, NUMERIC '1'))
AND (coalesce(mt.ra_lab_payment_amt, NUMERIC '0') = coalesce(ms.ra_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_lab_payment_amt, NUMERIC '1') = coalesce(ms.ra_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_operating_dsh_amt, NUMERIC '0') = coalesce(ms.ra_operating_dsh_amt, NUMERIC '0')
     AND coalesce(mt.ra_operating_dsh_amt, NUMERIC '1') = coalesce(ms.ra_operating_dsh_amt, NUMERIC '1'))
AND (coalesce(mt.ra_operating_idme_amt, NUMERIC '0') = coalesce(ms.ra_operating_idme_amt, NUMERIC '0')
     AND coalesce(mt.ra_operating_idme_amt, NUMERIC '1') = coalesce(ms.ra_operating_idme_amt, NUMERIC '1'))
AND (coalesce(mt.ra_operating_outlier_amt, NUMERIC '0') = coalesce(ms.ra_operating_outlier_amt, NUMERIC '0')
     AND coalesce(mt.ra_operating_outlier_amt, NUMERIC '1') = coalesce(ms.ra_operating_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.ra_outlier_amt, NUMERIC '0') = coalesce(ms.ra_outlier_amt, NUMERIC '0')
     AND coalesce(mt.ra_outlier_amt, NUMERIC '1') = coalesce(ms.ra_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.ra_therapy_payment_amt, NUMERIC '0') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_therapy_payment_amt, NUMERIC '1') = coalesce(ms.ra_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.ra_total_capital_amt, NUMERIC '0') = coalesce(ms.ra_total_capital_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_capital_amt, NUMERIC '1') = coalesce(ms.ra_total_capital_amt, NUMERIC '1'))
AND (coalesce(mt.ra_total_payment_amt, NUMERIC '0') = coalesce(ms.ra_total_payment_amt, NUMERIC '0')
     AND coalesce(mt.ra_total_payment_amt, NUMERIC '1') = coalesce(ms.ra_total_payment_amt, NUMERIC '1'))
AND (coalesce(mt.replace_device_reduction_amt, NUMERIC '0') = coalesce(ms.replace_device_reduction_amt, NUMERIC '0')
     AND coalesce(mt.replace_device_reduction_amt, NUMERIC '1') = coalesce(ms.replace_device_reduction_amt, NUMERIC '1'))
AND (coalesce(mt.rug_payment_amt, NUMERIC '0') = coalesce(ms.rug_payment_amt, NUMERIC '0')
     AND coalesce(mt.rug_payment_amt, NUMERIC '1') = coalesce(ms.rug_payment_amt, NUMERIC '1'))
AND (coalesce(mt.sequestration_amt, NUMERIC '0') = coalesce(ms.sequestration_amt, NUMERIC '0')
     AND coalesce(mt.sequestration_amt, NUMERIC '1') = coalesce(ms.sequestration_amt, NUMERIC '1'))
AND (coalesce(mt.short_stay_outlier_payment_amt, NUMERIC '0') = coalesce(ms.short_stay_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.short_stay_outlier_payment_amt, NUMERIC '1') = coalesce(ms.short_stay_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.tech_add_on_amt, NUMERIC '0') = coalesce(ms.tech_add_on_amt, NUMERIC '0')
     AND coalesce(mt.tech_add_on_amt, NUMERIC '1') = coalesce(ms.tech_add_on_amt, NUMERIC '1'))
AND (coalesce(mt.tech_add_on_payment_amt, NUMERIC '0') = coalesce(ms.tech_add_on_payment_amt, NUMERIC '0')
     AND coalesce(mt.tech_add_on_payment_amt, NUMERIC '1') = coalesce(ms.tech_add_on_payment_amt, NUMERIC '1'))
AND (coalesce(mt.therapy_charge_amt, NUMERIC '0') = coalesce(ms.therapy_charge_amt, NUMERIC '0')
     AND coalesce(mt.therapy_charge_amt, NUMERIC '1') = coalesce(ms.therapy_charge_amt, NUMERIC '1'))
AND (coalesce(mt.therapy_payment_amt, NUMERIC '0') = coalesce(ms.therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.therapy_payment_amt, NUMERIC '1') = coalesce(ms.therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_charge_amt, NUMERIC '0') = coalesce(ms.total_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_charge_amt, NUMERIC '1') = coalesce(ms.total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.total_expected_payment_amt, NUMERIC '0') = coalesce(ms.total_expected_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_expected_payment_amt, NUMERIC '1') = coalesce(ms.total_expected_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_opr_cptl_payment_amt, NUMERIC '0') = coalesce(ms.total_opr_cptl_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_opr_cptl_payment_amt, NUMERIC '1') = coalesce(ms.total_opr_cptl_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_payment_amt, NUMERIC '0') = coalesce(ms.total_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_payment_amt, NUMERIC '1') = coalesce(ms.total_payment_amt, NUMERIC '1'))
AND (coalesce(mt.transfer_payment_amt, NUMERIC '0') = coalesce(ms.transfer_payment_amt, NUMERIC '0')
     AND coalesce(mt.transfer_payment_amt, NUMERIC '1') = coalesce(ms.transfer_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_coinsurance_amt, NUMERIC '0') = coalesce(ms.var_coinsurance_amt, NUMERIC '0')
     AND coalesce(mt.var_coinsurance_amt, NUMERIC '1') = coalesce(ms.var_coinsurance_amt, NUMERIC '1'))
AND (coalesce(mt.var_contractual_amt, NUMERIC '0') = coalesce(ms.var_contractual_amt, NUMERIC '0')
     AND coalesce(mt.var_contractual_amt, NUMERIC '1') = coalesce(ms.var_contractual_amt, NUMERIC '1'))
AND (coalesce(mt.var_covered_charges_amt, NUMERIC '0') = coalesce(ms.var_covered_charges_amt, NUMERIC '0')
     AND coalesce(mt.var_covered_charges_amt, NUMERIC '1') = coalesce(ms.var_covered_charges_amt, NUMERIC '1'))
AND (coalesce(mt.var_deductible_amt, NUMERIC '0') = coalesce(ms.var_deductible_amt, NUMERIC '0')
     AND coalesce(mt.var_deductible_amt, NUMERIC '1') = coalesce(ms.var_deductible_amt, NUMERIC '1'))
AND (coalesce(mt.var_imed_educ_amt, NUMERIC '0') = coalesce(ms.var_imed_educ_amt, NUMERIC '0')
     AND coalesce(mt.var_imed_educ_amt, NUMERIC '1') = coalesce(ms.var_imed_educ_amt, NUMERIC '1'))
AND (coalesce(mt.var_lab_payment_amt, NUMERIC '0') = coalesce(ms.var_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_lab_payment_amt, NUMERIC '1') = coalesce(ms.var_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_operating_dsh_amt, NUMERIC '0') = coalesce(ms.var_operating_dsh_amt, NUMERIC '0')
     AND coalesce(mt.var_operating_dsh_amt, NUMERIC '1') = coalesce(ms.var_operating_dsh_amt, NUMERIC '1'))
AND (coalesce(mt.var_operating_ime_amt, NUMERIC '0') = coalesce(ms.var_operating_ime_amt, NUMERIC '0')
     AND coalesce(mt.var_operating_ime_amt, NUMERIC '1') = coalesce(ms.var_operating_ime_amt, NUMERIC '1'))
AND (coalesce(mt.var_operating_outlier_amt, NUMERIC '0') = coalesce(ms.var_operating_outlier_amt, NUMERIC '0')
     AND coalesce(mt.var_operating_outlier_amt, NUMERIC '1') = coalesce(ms.var_operating_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.var_outlier_amt, NUMERIC '0') = coalesce(ms.var_outlier_amt, NUMERIC '0')
     AND coalesce(mt.var_outlier_amt, NUMERIC '1') = coalesce(ms.var_outlier_amt, NUMERIC '1'))
AND (coalesce(mt.var_therapy_payment_amt, NUMERIC '0') = coalesce(ms.var_therapy_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_therapy_payment_amt, NUMERIC '1') = coalesce(ms.var_therapy_payment_amt, NUMERIC '1'))
AND (coalesce(mt.var_total_capital_amt, NUMERIC '0') = coalesce(ms.var_total_capital_amt, NUMERIC '0')
     AND coalesce(mt.var_total_capital_amt, NUMERIC '1') = coalesce(ms.var_total_capital_amt, NUMERIC '1'))
AND (coalesce(mt.var_total_payment_amt, NUMERIC '0') = coalesce(ms.var_total_payment_amt, NUMERIC '0')
     AND coalesce(mt.var_total_payment_amt, NUMERIC '1') = coalesce(ms.var_total_payment_amt, NUMERIC '1'))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        report_name,
        period_end_date,
        report_run_date_time,
        section_name,
        reporting_period,
        company_code,
        coid,
        facility_name,
        unit_num,
        pat_acct_num,
        patient_name,
        rate_schedule_name,
        section_month,
        cost_report_year,
        account_status_code,
        auto_posted_flag,
        cmg_code,
        drg_code,
        ra_drg_code,
        cmg_weight_amt,
        drg_weight_amt,
        prorated_drg_weight_amt,
        exp_hipps_cmg_code,
        ra_hipps_cmg_code,
        var_drg_flag,
        var_cmg_flag,
        discharge_admit_date,
        claim_submit_date,
        ra_payment_date,
        covered_days_los_cnt,
        days_billed_cnt,
        discharge_days_cnt,
        ra_covered_days_cnt,
        remit_cnt,
        var_covered_days_cnt,
        account_balance_amt,
        accrued_capital_subtotal_amt,
        accrued_operating_subtotal_amt,
        accrued_payment_amt,
        apc_charges_amt,
        apc_payment_amt,
        drg_arithmetic_los_amt,
        capital_payment_subtotal_amt,
        cmac_charge_amt,
        cmac_payment_amt,
        cmg_payment_amt,
        coinsurance_deductible_amt,
        computed_reimb_amt,
        cost_outlier_payment_amt,
        covered_charges_amt,
        drg_payment_amt,
        ect_payment_amt,
        education_payment_amt,
        fee_based_payment_amt,
        idme_payment_amt,
        lab_charge_amt,
        lab_payment_amt,
        lip_payment_amt,
        non_covered_charges_amt,
        operating_drg_payment_amt,
        operating_dsh_amt,
        operating_outlier_amt,
        operating_cost_outlier_amt,
        other_service_charge_amt,
        other_service_payment_amt,
        outlier_payment_amt,
        payment_adj_amt,
        ra_coinsurance_amt,
        ra_contractual_amt,
        ra_covered_charge_amt,
        ra_deductible_amt,
        ra_drg_amt,
        ra_idme_amt,
        ra_lab_payment_amt,
        ra_operating_dsh_amt,
        ra_operating_idme_amt,
        ra_operating_outlier_amt,
        ra_outlier_amt,
        ra_therapy_payment_amt,
        ra_total_capital_amt,
        ra_total_payment_amt,
        replace_device_reduction_amt,
        rug_payment_amt,
        sequestration_amt,
        short_stay_outlier_payment_amt,
        tech_add_on_amt,
        tech_add_on_payment_amt,
        therapy_charge_amt,
        therapy_payment_amt,
        total_charge_amt,
        total_expected_payment_amt,
        total_opr_cptl_payment_amt,
        total_payment_amt,
        transfer_payment_amt,
        var_coinsurance_amt,
        var_contractual_amt,
        var_covered_charges_amt,
        var_deductible_amt,
        var_imed_educ_amt,
        var_lab_payment_amt,
        var_operating_dsh_amt,
        var_operating_ime_amt,
        var_operating_outlier_amt,
        var_outlier_amt,
        var_therapy_payment_amt,
        var_total_capital_amt,
        var_total_payment_amt,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.patient_dw_id, ms.report_name, ms.period_end_date, ms.report_run_date_time, ms.section_name, ms.reporting_period, ms.company_code, ms.coid, ms.facility_name, ms.unit_num, ms.pat_acct_num, ms.patient_name, ms.rate_schedule_name, ms.section_month, ms.cost_report_year, ms.account_status_code, ms.auto_posted_flag, ms.cmg_code, ms.drg_code, ms.ra_drg_code, ms.cmg_weight_amt, ms.drg_weight_amt, ms.prorated_drg_weight_amt, ms.exp_hipps_cmg_code, ms.ra_hipps_cmg_code, ms.var_drg_flag, ms.var_cmg_flag, ms.discharge_admit_date, ms.claim_submit_date, ms.ra_payment_date, ms.covered_days_los_cnt, ms.days_billed_cnt, ms.discharge_days_cnt, ms.ra_covered_days_cnt, ms.remit_cnt, ms.var_covered_days_cnt, ms.account_balance_amt, ms.accrued_capital_subtotal_amt, ms.accrued_operating_subtotal_amt, ms.accrued_payment_amt, ms.apc_charges_amt, ms.apc_payment_amt, ms.drg_arithmetic_los_amt, ms.capital_payment_subtotal_amt, ms.cmac_charge_amt, ms.cmac_payment_amt, ms.cmg_payment_amt, ms.coinsurance_deductible_amt, ms.computed_reimb_amt, ms.cost_outlier_payment_amt, ms.covered_charges_amt, ms.drg_payment_amt, ms.ect_payment_amt, ms.education_payment_amt, ms.fee_based_payment_amt, ms.idme_payment_amt, ms.lab_charge_amt, ms.lab_payment_amt, ms.lip_payment_amt, ms.non_covered_charges_amt, ms.operating_drg_payment_amt, ms.operating_dsh_amt, ms.operating_outlier_amt, ms.operating_cost_outlier_amt, ms.other_service_charge_amt, ms.other_service_payment_amt, ms.outlier_payment_amt, ms.payment_adj_amt, ms.ra_coinsurance_amt, ms.ra_contractual_amt, ms.ra_covered_charge_amt, ms.ra_deductible_amt, ms.ra_drg_amt, ms.ra_idme_amt, ms.ra_lab_payment_amt, ms.ra_operating_dsh_amt, ms.ra_operating_idme_amt, ms.ra_operating_outlier_amt, ms.ra_outlier_amt, ms.ra_therapy_payment_amt, ms.ra_total_capital_amt, ms.ra_total_payment_amt, ms.replace_device_reduction_amt, ms.rug_payment_amt, ms.sequestration_amt, ms.short_stay_outlier_payment_amt, ms.tech_add_on_amt, ms.tech_add_on_payment_amt, ms.therapy_charge_amt, ms.therapy_payment_amt, ms.total_charge_amt, ms.total_expected_payment_amt, ms.total_opr_cptl_payment_amt, ms.total_payment_amt, ms.transfer_payment_amt, ms.var_coinsurance_amt, ms.var_contractual_amt, ms.var_covered_charges_amt, ms.var_deductible_amt, ms.var_imed_educ_amt, ms.var_lab_payment_amt, ms.var_operating_dsh_amt, ms.var_operating_ime_amt, ms.var_operating_outlier_amt, ms.var_outlier_amt, ms.var_therapy_payment_amt, ms.var_total_capital_amt, ms.var_total_payment_amt, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             report_name,
             period_end_date,
             report_run_date_time,
             section_name
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_gr_eom
      GROUP BY patient_dw_id,
               report_name,
               period_end_date,
               report_run_date_time,
               section_name
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `{{ params.param_parallon_ra_core_dataset_name }}`.cc_gr_eom');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;