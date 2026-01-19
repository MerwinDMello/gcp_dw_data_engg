DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/cc_denial.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/***************************************************************************
 Developer: Sean Wilson
      Name: CC_Denial BTEQ Script.
      Mod1: Creation of script on 10/9/2014. SW.
      Mod2: Added cast to Web_Disp_Code to match source and avoid truncation problems
            noted by QA on 11/21/2014 SW.
      Mod3: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
      Mod4: Added new columns Write_Off_Amt and Cash_Adjustment_Amt .  Also, updated Source_System_Code to get value
	        from staging table instead of hard-coded "N" on 8/26/2015 JAC.
      Mod5: added {{ params.param_parallon_ra_stage_dataset_name }}.V_EDW_Daily_Denial_Inventory in from on on 3/27/2016
      Mod6: Table unique primary index has two additional columns.  Changed script
            to match on Max_Appeal_Num and Max_Seq_Num on 4/25/2017 SW.
      Mod7: Added logic to populate 3 new columns Appeal_Level_Num,Appeal_Sent_Date,Prior_Appeal_Response_Date PT 1/9/2019
	Saravana -  PBI 24185 Changes - 12/3/2019
	Saravana -  PBI 24185 - 1/17/2020 - CASH_ADJUSTMENT_AMT revised to existing logic
      Mod10:  -  PBI 25628  - 3/23/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
 Mod: Added update statement to update Denial age because of Oracle issue 11232021
****************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA264;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory
SET denial_age = date_diff(edw_daily_denial_inventory.extract_date, edw_daily_denial_inventory.first_denial_date, DAY),
    last_activity_completion_age = date_diff(edw_daily_denial_inventory.extract_date, edw_daily_denial_inventory.last_activity_completion_date, DAY)
WHERE TRUE;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_core_dataset_name }}.cc_denial
SET denial_age_num = date_diff(DATE(cc_denial.extract_date_time), cc_denial.first_denial_date, DAY),
    last_actv_completion_age_num = date_diff(DATE(cc_denial.extract_date_time), cc_denial.last_actv_completion_date, DAY)
WHERE DATE(cc_denial.dw_last_update_date_time) > DATE '2021-11-20';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Locking EDWRA_STAGING.SSC_Denials For Access
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_denial AS x USING
  (SELECT a.patient_dw_id AS patient_dw_id,
          pyro.payor_dw_id AS payor_dw_id,
          sscd.payer_rank AS iplan_order_num,
          CAST(sscd.extract_date AS DATETIME) AS extract_date_time, -- SSCD.PAYER_RANK AS INSURANCE_ORDER_NUM,
 sscd.max_aplno AS max_appeal_num,
 sscd.max_seqno AS max_seq_num,
 sscd.accounting_period AS accounting_period_desc,
 sscd.ar_amount AS ar_amt,
 sscd.activity_due_date AS actv_due_date,
 sscd.activity_desc AS actv_desc,
 sscd.activity_subject AS actv_subject_title_desc,
 sscd.activity_owner AS actv_owner_name,
 sscd.admit_source AS admit_source_code,
 sscd.admit_type AS admit_type_ind,
 sscd.appeal_initiation_date AS legacy_appeal_initiation_date,
 a.company_code,
 sscd.coid,
 sscd.account_no AS pat_acct_num,
 sscd.patient_name,
 sscd.iplan_id,
 sscd.patient_dob AS patient_birth_date,
 sscd.insurance_provider_name,
 sscd.billing_name,
 sscd.billing_contact_person AS billing_contact_name,
 sscd.authorization_code AS authorization_code,
 sscd.payer_patient_id AS payor_patient_code,
 ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(sscd.pa_financial_class) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pa_financial_class_num,
 ROUND(sscd.payor_financial_class, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_num,
 sscd.major_payer_grp AS major_payor_group_code,
 sscd.billing_status AS billing_status_code,
 sscd.pa_service_code,
 sscd.pa_account_status AS pa_account_status_desc,
 sscd.pa_discharge_status AS pa_discharge_status_code,
 sscd.pa_patient_type AS pa_patient_type_desc,
 sscd.cancel_bill_ind,
 sscd.mon_account_payer_id AS monbot_acct_payer_id,
 sscd.latest_iplan_change_date_pa AS pa_latest_iplan_change_date,
 sscd.pa_drg AS pa_drg_code,
 CAST(sscd.pa_actual_los AS INT64) AS pa_actual_los_cnt,
 sscd.attending_physician_id,
 sscd.attending_physician_name,
 sscd.service_date_begin AS service_begin_date,
 sscd.discharge_date,
 sscd.pa_denial_update_date,
 sscd.cc_patient_type AS cc_patient_type_code,
 sscd.rate_schedule_name,
 sscd.rate_schedule_eff_begin_date,
 sscd.rate_schedule_eff_end_date,
 sscd.work_queue_name,
 sscd.comparison_method AS comparison_method_desc,
 sscd.project_name,
 sscd.reason_code,
 sscd.status_desc,
 sscd.status_category_desc,
 sscd.otd_to_date_amount_mtd AS category_to_date_amt,
 sscd.status_phase_desc,
 sscd.calc_date,
 sscd.total_charges AS total_charge_amt,
 sscd.total_billed_charges AS total_billed_charge_amt,
 sscd.covered_charges AS covered_charge_amt,
 sscd.total_expected_payment AS total_expected_payment_amt,
 sscd.total_expected_adjustment AS total_expected_adjustment_amt,
 sscd.total_pt_responsibility_actual AS total_pat_resp_actual_amt,
 sscd.total_variance_adjustment AS total_var_adjustment_amt,
 sscd.total_payments AS total_payment_amt,
 sscd.total_denial_amt AS total_denial_amt,
 sscd.payor_due_amt AS payor_due_amt,
 sscd.pa_total_account_bal AS pa_account_total_bal_amt,
 sscd.otd_to_date_amount_mtd AS otd_to_date_mtd_amt, --   PBI 24185
 sscd.otd_amt_net AS otd_net_amt, --   PBI 24185
 sscd.appeal_orig_amt AS appeal_orig_amt,
 sscd.current_appealed_amt AS current_appeal_amt,
 sscd.current_appeal_balance AS current_appeal_balance_amt,
 sscd.sequence_date_created AS appeal_seq_create_date,
 sscd.max_seq_deadline_date AS appeal_seq_deadline_date, --   PBI 24185 REMOVED
 sscd.close_date AS appeal_close_date,
 sscd.sequence_creator AS appeal_seq_creator_34_id,
 sscd.appeal_owner AS appeal_owner_34_id,
 sscd.appeal_modifier AS appeal_modifier_34_id,
 sscd.disp_code,
 sscd.disposition_code_modified_date AS disp_code_mod_date,
 sscd.disposition_code_modified_by AS disp_code_mod_user_id,
 substr(sscd.web_disposition_type, 1, 10) AS web_disp_type_code, --   PBI 24185 ASK FRAN
 substr(CAST(sscd.web_disp_code AS STRING), 1, 10) AS web_disp_code,
 sscd.seq_no AS appeal_seq_num,
 sscd.disp_desc,
 sscd.root_code,
 sscd.root_cause_description AS root_cause_desc,
 sscd.root_cause_dtl AS root_cause_dtl_desc,
 sscd.external_appeal_code,
 sscd.first_denial_date,
 sscd.denial_age AS denial_age_num,
 sscd.first_activity_create_date AS first_actv_create_date,
 sscd.last_activity_completion_date AS last_actv_completion_date,
 sscd.last_activity_completion_age AS last_actv_completion_age_num,
 sscd.last_user_activity_create_age AS last_user_act_create_age_num,
 sscd.last_remit_received_date AS last_remit_received_date, --  LAST REMIT DATE
 sscd.last_reason_change_date,
 sscd.last_status_change_date,
 sscd.last_project_change_date,
 sscd.last_owner_change_date,
 sscd.appeal_date_created AS appeal_create_date,
 sscd.apl_appeal_code AS appeal_code,
 sscd.apl_appeal_desc AS appeal_desc,
 sscd.appeal_sent_activity_ownr AS appeal_sent_actv_ownr_34_id,
 sscd.appeal_sent_activity_date AS appeal_sent_actv_date,
 sscd.appeal_sent_activity_age AS appeal_sent_actv_age_num,
 sscd.last_status_change_age AS last_status_change_age,
 sscd.appeal_sent_activity_subj AS appeal_sent_actv_subject_desc,
 sscd.appeal_sent_activity_desc AS appeal_sent_actv_desc,
 '' AS stratification_group_name,
 '' AS status_kpi_name,
 '' AS followup_kpi_name,
 '' AS appeal_sent_kpi_name,
 '' AS deadline_kpi_name,
 sscd.payer_category AS payor_category_name,
 sscd.payer_group_name AS payor_group_name, --     
 sscd.writeoff_amt_mtd as mtd_writeoff_amt , --  pbi 24185
 sscd.new_appeal_flag AS new_appeal_id, --   PBI 24185
 sscd.writeoff_amt_mtd as write_off_amt, -- pbi 24185
 sscd.writeoff_amt_net as  net_denial_write_off_amt, -- pbi 24185
 sscd.cash_adj_amt_net AS field_cash_adj_amt_net, --   PBI 24185
 --  SSCD.OTD_NET_AMT AS OTD_AMT_NET ,-- PBI 24185
 sscd.cash_adj_amt_net AS cash_adj_amt_net, --   PBI 24185
 sscd.cash_adj_amt_mtd AS cash_adj_amt_mtd, --   PBI 24185
 sscd.cash_adj_amt_mtd AS cash_adjustment_amt, --  PBI 24185 PBI 24185
 sscd.apl_lvl AS appeal_level_num, --   PBI 24185
 sscd.apl_sent_dt AS appeal_sent_date, --   PBI 24185
 sscd.prior_apl_rspn_dt AS prior_appeal_response_date, --   PBI 24185
 sscd.expected_amt AS expected_amt, --  PBI 24185
 sscd.facility_name AS facility_name, --  PBI 24185 ,
 sscd.remit_drg_code AS remit_drg_code, --  PBI 24185
 sscd.ssc_id AS ssc_id, --  PBI 24185
 sscd.ssc_name AS ssc_name, --  PBI 24185
 sscd.unit_num AS unit_num, --  PBI 24185
 sscd.vendor_code AS vendor_code, --  PBI 24185
 sscd.appeal_deadline_days_remaining AS appeal_deadline_rmd_days_num, --  PBI 24185
 sscd.otd_amt_net AS otd_amt_net, --  PBI 24185
 sscd.otd_to_date_amount_mtd AS otd_to_date_amount_mtd, --  PBI 24185
 sscd.disposition_code_modified_by AS disposition_code_modified_by, --  PBI 24185
 sscd.disposition_code_modified_date AS disposition_code_modified_date, --  PBI 24185
 sscd.latest_iplan_change_date_pa AS latest_iplan_change_date_pa, --  PBI 24185
 sscd.mon_account_payer_id AS mon_account_payer_id, --  PBI 24185
 sscd.new_appeal_flag AS new_appeal_ind, --  PBI 24185
 --   PBI 24185 NEW FIELDS
 --  last remit date to be added
 sscd.artiva_activity_due_date AS artiva_open_activity_due_date,
 sscd.calc_base AS calc_base_desc,
 sscd.schema_id AS schema_id, -- ,SSCD.Seq_No AS Sequence_Num
 sscd.sub_unit_num AS sub_unit_num,
 sscd.cash_adj_amt_mtd AS cash_adjustment_mtd_amt,
 sscd.cash_adj_amt_net AS cash_adjustment_net_amt,
 sscd.row_count AS daily_denial_inventory_count,
 CASE
     WHEN sscd.schema_id = 0 THEN 'P'
     ELSE 'N'
 END AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.edw_daily_denial_inventory AS sscd
   INNER JOIN --  CHANGE TO STAGING
 {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rcos ON upper(rtrim(rcos.coid)) = upper(rtrim(sscd.coid))
   AND rcos.schema_id = sscd.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS a ON upper(rtrim(a.coid)) = upper(rtrim(sscd.coid))
   AND upper(rtrim(a.company_code)) = upper(rtrim(rcos.company_code))
   AND a.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(sscd.account_no) AS FLOAT64)
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(sscd.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(rcos.company_code))
   AND pyro.iplan_id = sscd.iplan_id
   WHERE sscd.schema_id IN(1,
                           3) ) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.iplan_order_num = z.iplan_order_num
AND x.extract_date_time = z.extract_date_time
AND x.max_appeal_num = z.max_appeal_num
AND x.max_seq_num = z.max_seq_num WHEN MATCHED THEN
UPDATE
SET accounting_period_desc = substr(z.accounting_period_desc, 1, 50),
    ar_amt = z.ar_amt,
    actv_due_date = z.actv_due_date,
    actv_desc = substr(z.actv_desc, 1, 4000),
    actv_subject_title_desc = z.actv_subject_title_desc,
    actv_owner_name = z.actv_owner_name,
    admit_source_code = z.admit_source_code,
    admit_type_ind = z.admit_type_ind,
    legacy_appeal_initiation_date = z.legacy_appeal_initiation_date,
    appeal_orig_amt = z.appeal_orig_amt,
    appeal_seq_create_date = z.appeal_seq_create_date,
    appeal_seq_deadline_date = z.appeal_seq_deadline_date,
    appeal_close_date = z.appeal_close_date,
    appeal_seq_creator_34_id = substr(z.appeal_seq_creator_34_id, 1, 50),
    appeal_owner_34_id = substr(z.appeal_owner_34_id, 1, 50),
    appeal_modifier_34_id = substr(z.appeal_modifier_34_id, 1, 50),
    appeal_code = z.appeal_code,
    appeal_desc = z.appeal_desc,
    appeal_create_date = z.appeal_create_date,
    appeal_deadline_rmd_days_num = z.appeal_deadline_rmd_days_num,
    appeal_sent_actv_age_num = z.appeal_sent_actv_age_num,
    appeal_sent_actv_date = z.appeal_sent_actv_date,
    appeal_sent_actv_desc = substr(z.appeal_sent_actv_desc, 1, 4000),
    appeal_sent_actv_ownr_34_id = z.appeal_sent_actv_ownr_34_id,
    appeal_sent_actv_subject_desc = z.appeal_sent_actv_subject_desc,
    appeal_seq_num = z.appeal_seq_num,
    appeal_sent_kpi_name = substr(z.appeal_sent_kpi_name, 1, 55),
    artiva_open_activity_due_date = z.artiva_open_activity_due_date,
    attending_physician_id = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.attending_physician_id) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    attending_physician_name = z.attending_physician_name,
    authorization_code = z.authorization_code,
    billing_name = substr(z.billing_name, 1, 50),
    billing_contact_name = substr(z.billing_contact_name, 1, 50),
    billing_status_code = substr(z.billing_status_code, 1, 1),
    calc_base_desc = z.calc_base_desc,
    calc_date = z.calc_date,
    cancel_bill_ind = substr(z.cancel_bill_ind, 1, 1),
    cash_adjustment_amt = z.cash_adjustment_amt,
    cash_adjustment_mtd_amt = z.cash_adjustment_mtd_amt,
    cash_adjustment_net_amt = z.cash_adjustment_net_amt,
    category_to_date_amt = z.category_to_date_amt,
    cc_patient_type_code = z.cc_patient_type_code,
    company_code = z.company_code,
    coid = z.coid,
    comparison_method_desc = substr(z.comparison_method_desc, 1, 40),
    covered_charge_amt = z.covered_charge_amt,
    current_appeal_amt = z.current_appeal_amt,
    current_appeal_balance_amt = z.current_appeal_balance_amt,
    deadline_kpi_name = substr(z.deadline_kpi_name, 1, 55),
    denial_age_num = z.denial_age_num,
    disp_code = z.disp_code,
    disp_code_mod_date = z.disp_code_mod_date,
    disp_code_mod_user_id = substr(z.disp_code_mod_user_id, 1, 20),
    disp_desc = z.disp_desc,
    discharge_date = z.discharge_date,
    expected_amt = z.expected_amt,
    external_appeal_code = z.external_appeal_code,
    facility_name = z.facility_name,
    first_actv_create_date = z.first_actv_create_date,
    first_denial_date = z.first_denial_date,
    followup_kpi_name = substr(z.followup_kpi_name, 1, 10),
    iplan_id = z.iplan_id,
    insurance_provider_name = z.insurance_provider_name,
    last_actv_completion_age_num = z.last_actv_completion_age_num,
    last_actv_completion_date = z.last_actv_completion_date,
    last_owner_change_date = z.last_owner_change_date,
    last_project_change_date = z.last_project_change_date,
    last_reason_change_date = z.last_reason_change_date,
    last_status_change_date = z.last_status_change_date,
    last_remit_received_date = DATE(z.last_remit_received_date),
    last_user_act_create_age_num = z.last_user_act_create_age_num,
    major_payor_group_code = substr(z.major_payor_group_code, 1, 10),
    monbot_acct_payer_id = z.monbot_acct_payer_id,
    mtd_writeoff_amt = z.mtd_writeoff_amt,
    net_denial_write_off_amt = z.net_denial_write_off_amt,
    new_appeal_ind = z.new_appeal_ind,
    otd_net_amt = z.otd_net_amt,
    otd_to_date_mtd_amt = z.otd_to_date_mtd_amt,
    pa_financial_class_num = z.pa_financial_class_num,
    pa_service_code = z.pa_service_code,
    pa_account_status_desc = z.pa_account_status_desc,
    pa_discharge_status_code = z.pa_discharge_status_code,
    pa_patient_type_desc = z.pa_patient_type_desc,
    pa_latest_iplan_change_date = z.pa_latest_iplan_change_date,
    pa_drg_code = z.pa_drg_code,
    pa_actual_los_cnt = z.pa_actual_los_cnt,
    pa_denial_update_date = z.pa_denial_update_date,
    pa_account_total_bal_amt = z.pa_account_total_bal_amt,
    pat_acct_num = ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'),
    patient_name = substr(z.patient_name, 1, 50),
    patient_birth_date = z.patient_birth_date,
    payor_category_name = substr(z.payor_category_name, 1, 15),
    payor_patient_code = z.payor_patient_code,
    payor_financial_class_num = z.payor_financial_class_num,
    payor_group_name = substr(z.payor_group_name, 1, 250),
    payor_due_amt = z.payor_due_amt,
    project_name = z.project_name,
    rate_schedule_name = z.rate_schedule_name,
    rate_schedule_eff_begin_date = z.rate_schedule_eff_begin_date,
    rate_schedule_eff_end_date = z.rate_schedule_eff_end_date,
    reason_code = z.reason_code,
    remit_drg_code = substr(z.remit_drg_code, 1, 5),
    root_cause_desc = z.root_cause_desc,
    root_cause_dtl_desc = substr(z.root_cause_dtl_desc, 1, 100),
    root_code = z.root_code,
    schema_id = z.schema_id,
    service_begin_date = z.service_begin_date,
    ssc_id = z.ssc_id,
    ssc_name = substr(z.ssc_name, 1, 100),
    status_desc = z.status_desc,
    status_category_desc = substr(z.status_category_desc, 1, 10),
    status_phase_desc = substr(z.status_phase_desc, 1, 50),
    stratification_group_name = substr(z.stratification_group_name, 1, 55),
    status_kpi_name = substr(z.status_kpi_name, 1, 55),
    sub_unit_num = substr(z.sub_unit_num, 1, 5),
    total_billed_charge_amt = z.total_billed_charge_amt,
    total_charge_amt = z.total_charge_amt,
    total_denial_amt = z.total_denial_amt,
    total_expected_payment_amt = z.total_expected_payment_amt,
    total_expected_adjustment_amt = z.total_expected_adjustment_amt,
    total_pat_resp_actual_amt = z.total_pat_resp_actual_amt,
    total_payment_amt = z.total_payment_amt,
    total_var_adjustment_amt = z.total_var_adjustment_amt,
    unit_num = substr(z.unit_num, 1, 5),
    vendor_code = z.vendor_code,
    web_disp_code = z.web_disp_code,
    web_disp_type_code = substr(z.web_disp_type_code, 1, 1),
    write_off_amt = z.write_off_amt,
    work_queue_name = z.work_queue_name,
    appeal_level_num = CAST(z.appeal_level_num AS INT64),
    appeal_sent_date = z.appeal_sent_date,
    prior_appeal_response_date = z.prior_appeal_response_date,
    daily_denial_inventory_count = z.daily_denial_inventory_count,
    source_system_code = substr(z.source_system_code, 1, 1),
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        iplan_order_num,
        extract_date_time,
        max_appeal_num,
        max_seq_num,
        accounting_period_desc,
        ar_amt,
        actv_due_date,
        actv_desc,
        actv_subject_title_desc,
        actv_owner_name,
        admit_source_code,
        admit_type_ind,
        legacy_appeal_initiation_date,
        appeal_orig_amt,
        appeal_seq_create_date,
        appeal_seq_deadline_date,
        appeal_close_date,
        appeal_seq_creator_34_id,
        appeal_owner_34_id,
        appeal_modifier_34_id,
        appeal_code,
        appeal_desc,
        appeal_create_date,
        appeal_deadline_rmd_days_num,
        appeal_sent_actv_age_num,
        appeal_sent_actv_date,
        appeal_sent_actv_desc,
        appeal_sent_actv_ownr_34_id,
        appeal_sent_actv_subject_desc,
        appeal_seq_num,
        appeal_sent_kpi_name,
        artiva_open_activity_due_date,
        attending_physician_id,
        attending_physician_name,
        authorization_code,
        billing_name,
        billing_contact_name,
        billing_status_code,
        calc_base_desc,
        calc_date,
        cancel_bill_ind,
        cash_adjustment_amt,
        cash_adjustment_mtd_amt,
        cash_adjustment_net_amt,
        category_to_date_amt,
        cc_patient_type_code,
        company_code,
        coid,
        comparison_method_desc,
        covered_charge_amt,
        current_appeal_amt,
        current_appeal_balance_amt,
        deadline_kpi_name,
        denial_age_num,
        disp_code,
        disp_code_mod_date,
        disp_code_mod_user_id,
        disp_desc,
        discharge_date,
        expected_amt,
        external_appeal_code,
        facility_name,
        first_actv_create_date,
        first_denial_date,
        followup_kpi_name,
        iplan_id,
        insurance_provider_name,
        last_actv_completion_age_num,
        last_actv_completion_date,
        last_owner_change_date,
        last_project_change_date,
        last_reason_change_date,
        last_status_change_date,
        last_remit_received_date,
        last_user_act_create_age_num,
        major_payor_group_code,
        monbot_acct_payer_id,
        mtd_writeoff_amt,
        net_denial_write_off_amt,
        new_appeal_ind,
        otd_net_amt,
        otd_to_date_mtd_amt,
        pa_financial_class_num,
        pa_service_code,
        pa_account_status_desc,
        pa_discharge_status_code,
        pa_patient_type_desc,
        pa_latest_iplan_change_date,
        pa_drg_code,
        pa_actual_los_cnt,
        pa_denial_update_date,
        pa_account_total_bal_amt,
        pat_acct_num,
        patient_name,
        patient_birth_date,
        payor_category_name,
        payor_patient_code,
        payor_financial_class_num,
        payor_group_name,
        payor_due_amt,
        project_name,
        rate_schedule_name,
        rate_schedule_eff_begin_date,
        rate_schedule_eff_end_date,
        reason_code,
        remit_drg_code,
        root_cause_desc,
        root_cause_dtl_desc,
        root_code,
        schema_id,
        service_begin_date,
        ssc_id,
        ssc_name,
        status_desc,
        status_category_desc,
        status_phase_desc,
        stratification_group_name,
        status_kpi_name,
        sub_unit_num,
        total_billed_charge_amt,
        total_charge_amt,
        total_denial_amt,
        total_expected_payment_amt,
        total_expected_adjustment_amt,
        total_pat_resp_actual_amt,
        total_payment_amt,
        total_var_adjustment_amt,
        unit_num,
        vendor_code,
        web_disp_code,
        web_disp_type_code,
        write_off_amt,
        work_queue_name,
        appeal_level_num,
        appeal_sent_date,
        prior_appeal_response_date,
        daily_denial_inventory_count,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.patient_dw_id, z.payor_dw_id, z.iplan_order_num, z.extract_date_time, z.max_appeal_num, z.max_seq_num, substr(z.accounting_period_desc, 1, 50), z.ar_amt, z.actv_due_date, substr(z.actv_desc, 1, 4000), z.actv_subject_title_desc, z.actv_owner_name, z.admit_source_code, z.admit_type_ind, z.legacy_appeal_initiation_date, z.appeal_orig_amt, z.appeal_seq_create_date, z.appeal_seq_deadline_date, z.appeal_close_date, substr(z.appeal_seq_creator_34_id, 1, 50), substr(z.appeal_owner_34_id, 1, 50), substr(z.appeal_modifier_34_id, 1, 50), z.appeal_code, z.appeal_desc, z.appeal_create_date, z.appeal_deadline_rmd_days_num, z.appeal_sent_actv_age_num, z.appeal_sent_actv_date, substr(z.appeal_sent_actv_desc, 1, 4000), z.appeal_sent_actv_ownr_34_id, z.appeal_sent_actv_subject_desc, z.appeal_seq_num, substr(z.appeal_sent_kpi_name, 1, 55), z.artiva_open_activity_due_date, ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.attending_physician_id) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), z.attending_physician_name, z.authorization_code, substr(z.billing_name, 1, 50), substr(z.billing_contact_name, 1, 50), substr(z.billing_status_code, 1, 1), z.calc_base_desc, z.calc_date, substr(z.cancel_bill_ind, 1, 1), z.cash_adjustment_amt, z.cash_adjustment_mtd_amt, z.cash_adjustment_net_amt, z.category_to_date_amt, z.cc_patient_type_code, z.company_code, z.coid, substr(z.comparison_method_desc, 1, 40), z.covered_charge_amt, z.current_appeal_amt, z.current_appeal_balance_amt, substr(z.deadline_kpi_name, 1, 55), z.denial_age_num, z.disp_code, z.disp_code_mod_date, substr(z.disp_code_mod_user_id, 1, 20), z.disp_desc, z.discharge_date, z.expected_amt, z.external_appeal_code, z.facility_name, z.first_actv_create_date, z.first_denial_date, substr(z.followup_kpi_name, 1, 10), z.iplan_id, z.insurance_provider_name, z.last_actv_completion_age_num, z.last_actv_completion_date, z.last_owner_change_date, z.last_project_change_date, z.last_reason_change_date, z.last_status_change_date, DATE(z.last_remit_received_date), z.last_user_act_create_age_num, substr(z.major_payor_group_code, 1, 10), z.monbot_acct_payer_id, z.mtd_writeoff_amt, z.net_denial_write_off_amt, z.new_appeal_ind, z.otd_net_amt, z.otd_to_date_mtd_amt, z.pa_financial_class_num, z.pa_service_code, z.pa_account_status_desc, z.pa_discharge_status_code, z.pa_patient_type_desc, z.pa_latest_iplan_change_date, z.pa_drg_code, z.pa_actual_los_cnt, z.pa_denial_update_date, z.pa_account_total_bal_amt, ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(z.pat_acct_num) AS NUMERIC), 0, 'ROUND_HALF_EVEN'), substr(z.patient_name, 1, 50), z.patient_birth_date, substr(z.payor_category_name, 1, 15), z.payor_patient_code, z.payor_financial_class_num, substr(z.payor_group_name, 1, 250), z.payor_due_amt, z.project_name, z.rate_schedule_name, z.rate_schedule_eff_begin_date, z.rate_schedule_eff_end_date, z.reason_code, substr(z.remit_drg_code, 1, 5), z.root_cause_desc, substr(z.root_cause_dtl_desc, 1, 100), z.root_code, z.schema_id, z.service_begin_date, z.ssc_id, substr(z.ssc_name, 1, 100), z.status_desc, substr(z.status_category_desc, 1, 10), substr(z.status_phase_desc, 1, 50), substr(z.stratification_group_name, 1, 55), substr(z.status_kpi_name, 1, 55), substr(z.sub_unit_num, 1, 5), z.total_billed_charge_amt, z.total_charge_amt, z.total_denial_amt, z.total_expected_payment_amt, z.total_expected_adjustment_amt, z.total_pat_resp_actual_amt, z.total_payment_amt, z.total_var_adjustment_amt, substr(z.unit_num, 1, 5), z.vendor_code, z.web_disp_code, substr(z.web_disp_type_code, 1, 1), z.write_off_amt, z.work_queue_name, CAST(z.appeal_level_num AS INT64), z.appeal_sent_date, z.prior_appeal_response_date, z.daily_denial_inventory_count, substr(z.source_system_code, 1, 1), z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             iplan_order_num,
             extract_date_time,
             max_appeal_num,
             max_seq_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_denial
      GROUP BY patient_dw_id,
               payor_dw_id,
               iplan_order_num,
               extract_date_time,
               max_appeal_num,
               max_seq_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_denial');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- -  PBI 24185 SELECT END
-- Patient_DW_Id 	= Z.Patient_DW_Id 	,
--  Payor_DW_Id	= Z.    Payor_DW_Id	,
-- IPlan_Order_Num	= Z.    IPlan_Order_Num	,
-- Extract_Date_Time	= Z.    Extract_Date_Time	,
-- Max_Appeal_Num	= Z.    Max_Appeal_Num	,
--  Max_Seq_Num	= Z.    Max_Seq_Num	,
-- Sequence_Num	= Z.    Sequence_Num	,
-- Sequence_Num,
-- Z.Sequence_Num,
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


UPDATE {{ params.param_parallon_ra_core_dataset_name }}.cc_denial
SET denial_age_num = date_diff(DATE(cc_denial.extract_date_time), cc_denial.first_denial_date, DAY),
    last_actv_completion_age_num = date_diff(DATE(cc_denial.extract_date_time), cc_denial.last_actv_completion_date, DAY)
WHERE DATE(cc_denial.dw_last_update_date_time) > DATE '2021-11-20';


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA','CC_Denial');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;