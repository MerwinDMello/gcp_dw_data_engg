DECLARE DUP_COUNT INT64;
DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;
-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_account_activity.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************
  Developer: Sean Wilson
       Name: CC_Account_Payor.sql
       Date: 10/13/2014
       Mod1: Fix for incorrect column name on 11/11/2014 SW.
       Mod2: Changed join from WQ_Profile_Account to WQ_Org_Account for
             unique primary key on 11/19/2014 SW.
       Mod3: Addressed QA defects on 12/2/2014 SW.
       Mod4: Changed cast to trim for billing_phone_num and _fax_num on
             12/4/2014 based on QA SW.
       Mod5: Changed cast to trim for billing_zip_code on 12/5/2014 based on
             QA results SW.
       Mod6: Turned on diagnostics because Teradata sucks from time to time on
             12/22/2014 SW.
  Mod7: Removed CAST on Patient Account Number on 1/13/2014. AS.
  Mod8:Changed Query Band Statement to have Audit job name for increase in priority on
  teradata side and ease of understanding for DBA's on 9/22/2018 PT.
  Mod9: 3/1/2020 PBI 25190 - Audit Merge - Saravana Moorthy
  Mod10: 9/21/2022 PCO-914 - Added Payer_Type_Code column- PT
*****************************************************************************/
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage;

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

insert into {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage (account_calc_situation_ind, account_payor_id, account_payor_status_id, actual_pat_responsibility_amt, allow_contract_code_change_ind, apc_group_ind, appeal_id, authorization_cd, autm_post_amt, bill_reason_code, billed_date, billing_address_1, billing_address_2, billing_city_name, billing_contact_email_name, billing_contact_person_name, billing_fax_num, billing_name, billing_phone_num_cd, billing_state_code, billing_zip_code, calc_base_choice_resolved_ind, calc_base_id, calc_base_survivor_id, calc_date, calc_lock_ind, calc_num, calc_result_ind, cers_profile_id, cers_term_id, change_claim_trigger_ind, cmpt_method_choice_id, coid, coinsurance_amt, company_code, copay_amt, covered_charge_instu_amt, creation_date, current_exp_contractual_amt, current_exp_payment_amt, deductible_amt, drg_version, dw_last_update_date_time, eligible_ind, employer_name, est_pat_responsibility_amt, external_appeal_code, first_denial_date, insurance_group_name, insurance_group_num_cd, insured_birth_date, insured_gender_cd, insured_name, interest_stop_date, iplan_id, iplan_order_num, life_check_eligible_ind, manual_trigger_ind, pa_autm_post_ind, pa_blood_deductible_amt, pa_coinsurance_amt, pa_denial_date, pa_financial_class_code, pa_prof_part_b_charge_amt, pat_acct_num, patient_dw_id, payor_dw_id, payor_identification_num_cd, prof_covered_charge_amt, prof_exp_payment_amt, project_id, rate_schedule_name, reason_id, relation_to_insured_pat_cd, source_system_code, sqstrtn_reduction_amt, total_adjustment_amt, total_denial_amt, total_exp_contractual_amt, total_exp_payment_amt, total_pat_responsibility_amt, total_payment_amt, total_variance_adjustment_amt, unit_num, update_date, work_queue_name, capping_method_id, prepay_mgd_care_sw, payer_type_code) -- --msk pbi 25190

select z.account_calc_situation_ind as account_calc_situation_ind,
       z.account_payor_id as account_payor_id,
       z.account_payer_status_id as account_payor_status_id,
       z.actual_pt_responsibility as actual_pat_responsibility_amt,
       z.allow_contract_change_ind as allow_contract_code_change_ind,
       z.apc_group_ind as apc_group_ind,
       z.apl_appeal_id as appeal_id,
       z.authorization_code as authorization_cd,
       z.auto_post_amt as autm_post_amt,
       z.bill_reason_code as bill_reason_code,
       z.billed_date as billed_date,
       z.billing_address1 as billing_address_1,
       z.billing_address2 as billing_address_2,
       z.billing_city as billing_city_name,
       z.billing_contact_email as billing_contact_email_name,
       z.billing_contact_person as billing_contact_person_name,
       z.billing_fax as billing_fax_num,
       z.billing_name as billing_name,
       z.billing_phone as billing_phone_num_cd,
       z.billing_state as billing_state_code,
       z.billing_zip as billing_zip_code,
       z.calc_base_choice_resolved_ind as calc_base_choice_resolved_ind,
       z.calc_base_id as calc_base_id,
       z.calc_base_survivor_id as calc_base_survivor_id,
       z.calc_date as calc_date,
       z.calc_lock_ind as calc_lock_ind,
       CAST(z.calc_num AS INT64) as calc_num,
       z.calc_result_ind as calc_result_ind,
       z.cers_profile_id as cers_profile_id,
       z.cers_term_id as cers_term_id,
       cast(z.change_837_trigger_ind as string) as change_claim_trigger_ind,
       z.comp_method_choice as cmpt_method_choice_id,
       z.coid as coid,
       z.coinsurance_amt as coinsurance_amt,
       z.company_code as company_code,
       z.copay_amt as copay_amt,
       z.covered_charges_inst_amt as covered_charge_instu_amt,
       z.creation_date as creation_date,
       z.current_exp_contractual_amt as current_exp_contractual_amt,
       z.current_exp_pmt_amt as current_exp_payment_amt,
       z.deductible_amt as deductible_amt,
       z.drg_version as drg_version,
       datetime_trunc(current_datetime('US/Central'), SECOND) as dw_last_update_date_time,
       z.is_eligible_ind as eligible_ind,
       z.employer_name as employer_name,
       z.estimated_pt_responsibility as est_pat_responsibility_amt,
       z.external_appeal_code as external_appeal_code,
       z.first_denial_date as first_denial_date,
       z.insurance_group_name as insurance_group_name,
       z.insurance_group_num as insurance_group_num_cd,
       z.insured_date_of_birth as insured_birth_date,
       z.insured_gender as insured_gender_cd,
       z.insured_name as insured_name,
       z.interest_stop_date as interest_stop_date,
       z.iplan_id as iplan_id,
       CAST(z.iplan_order_num AS INT64) as iplan_order_num,
       z.life_check_eligible_ind as life_check_eligible_ind,
       cast(z.manual_trigger_ind as string) as manual_trigger_ind,
       z.pa_auto_post_ind as pa_autm_post_ind,
       z.pa_blood_deductible_amt as pa_blood_deductible_amt,
       z.pa_coinsurance_amt as pa_coinsurance_amt,
       z.pa_denial_date as pa_denial_date,
       z.pa_financial_class_code as pa_financial_class_code,
       z.pa_prof_part_b_charge_amt as pa_prof_part_b_charge_amt,
       z.pat_acct_num as pat_acct_num,
       z.patient_dw_id as patient_dw_id,
       z.payor_dw_id as payor_dw_id,
       z.payer_identification_num as payor_identification_num_cd,
       z.prof_covered_charges_amt as prof_covered_charge_amt,
       z.prof_exp_payment_amt as prof_exp_payment_amt,
       z.project_id as project_id,
       z.rate_schedule_name as rate_schedule_name,
       z.reason_id as reason_id,
       z.relation_to_insured as relation_to_insured_pat_cd,
       'n' as source_system_code,
       z.seq_red_amt as sqstrtn_reduction_amt,
       z.total_adjustment_amt as total_adjustment_amt,
       z.total_denial_amt as total_denial_amt,
       z.total_expected_contractual_amt as total_exp_contractual_amt,
       z.total_exp_payment_amt as total_exp_payment_amt,
       z.total_pt_responsibility_amt as total_pat_responsibility_amt,
       z.total_payment_amt as total_payment_amt,
       z.total_variance_adjustment as total_variance_adjustment_amt,
       z.unit_num as unit_num,
       z.update_date as update_date,
       z.wq_name as work_queue_name,
       z.cob_method_id as capping_method_id,
       z.ppmc_ind as prepay_mgd_care_sw,
       z.payer_type_code as payer_type_code
from
  (select b.patient_dw_id,
          c.payor_dw_id,
          a.company_code,
          a.coid,
          a.unit_num,
          a.insurance_order_num as iplan_order_num,
          a.pat_acct_num,
          a.iplan_id,
          a.wq_name,
          a.authorization_code,
          a.employer_name,
          a.insurance_group_name,
          a.insurance_group_num,
          a.insured_name,
          a.payer_identification_num,
          a.rel_to_insured,
          a.billed_date,
          a.date_created as creation_date,
          a.date_updated as update_date,
          a.billing_name,
          a.billing_address1,
          a.billing_address2,
          a.billing_city,
          a.billing_state,
          a.billing_zip,
          a.billing_phone,
          a.billing_contact_person,
          a.billing_fax,
          a.billing_contact_email,
          a.relation_to_insured,
          a.calculation_date as calc_date,
          a.is_eligible_ind,
          a.insured_gender,
          a.insured_date_of_birth,
          a.pa_financial_class_code,
          a.drg_version_code as drg_version,
          a.pa_auto_post_ind,
          a.bill_reason_code,
          a.pa_denial_update_date as pa_denial_date,
          a.apc_group_ind,
          a.allow_contract_change_ind,
          a.calc_lock_ind,
          a.cers_profile_id,
          a.calculation_num as calc_num,
          a.rate_schedule_name,
          a.cers_term_id,
          a.calc_base_id,
          a.interest_stop_date,
          a.calc_base_survivor_ind as calc_base_survivor_id,
          a.comp_method_choice,
          a.calc_result_ind,
          a.calc_base_choice_resolved as calc_base_choice_resolved_ind,
          a.account_calc_situation as account_calc_situation_ind,
          a.reason_id,
          a.account_payer_status_id,
          a.project_id,
          a.cob_method_id,
          a.apl_appeal_id,
          a.change_837_trigger_ind,
          a.manual_trigger_ind,
          a.life_check_eligible_ind,
          a.external_appeal_code,
          a.first_denial_date,
          a.coinsurance_amt,
          a.deductible_amt,
          a.copay_amt,
          a.covered_charges_inst_amt,
          a.total_exp_payment_amt,
          a.total_payment_amt,
          a.total_pt_responsibility_amt,
          a.current_exp_pmt_amt,
          a.current_exp_contractual_amt,
          a.total_adjustment_amount as total_adjustment_amt,
          a.total_expected_contractual_amt,
          a.pa_prof_part_b_charge_amt,
          a.pa_blood_deductible_amt,
          a.auto_post_amt,
          a.prof_covered_charges_amt,
          a.total_variance_adjustment,
          a.estimated_pt_responsibility,
          a.actual_pt_responsibility,
          a.pa_coinsurance_amt,
          a.total_denial_amt,
          a.prof_exp_payment_amt,
          a.seq_red_amt,
          a.account_payor_id,
          a.ppmc_ind,
          a.pyr_type as payer_type_code
   from
     (select rccos.company_code,
             rccos.coid,
             rccos.unit_num,
             mapyr.payer_rank as insurance_order_num,
             ma.account_no as pat_acct_num,
             case
                 when trim(mpyr.code) = 'NO INS' then 0
                 else cast(substr(trim(mpyr.code), 1, 3)|| substr(trim(mpyr.code), 5, 2) as integer)
             end as iplan_id,
             wqpf.name as wq_name,
             mapyr.authorization_code,
             mapyr.employer_name,
             mapyr.group_name as insurance_group_name,
             mapyr.insurance_group_no as insurance_group_num,
             mapyr.insured_name,
             mapyr.payer_identification_no as payer_identification_num,
             mapyr.rel_to_insured as rel_to_insured,
             mapyr.billed_date,
             mapyr.date_created,
             mapyr.date_updated,
             mapyr.billing_name,
             mapyr.billing_address1,
             mapyr.billing_address2,
             mapyr.billing_city,
             mapyr.billing_state,
             trim(mapyr.billing_zip) as billing_zip,
             trim(mapyr.billing_phone) as billing_phone,
             mapyr.billing_contact_person,
             trim(mapyr.billing_fax) as billing_fax,
             mapyr.billing_contact_email,
             mapyr.rel_to_insured as relation_to_insured,
             mapyr.calculation_date,
             case
                 when mapyr.is_eligible = 1 then 'Y'
                 else 'N'
             end as is_eligible_ind,
             mapyr.insureds_gender as insured_gender,
             mapyr.insureds_date_of_birth as insured_date_of_birth,
             mapyr.misc_char01 as pa_financial_class_code,
             mapyr.misc_char02 as drg_version_code,
             mapyr.misc_char03 as pa_auto_post_ind,
             mapyr.misc_char04 as bill_reason_code,
             mapyr.misc_date01 as pa_denial_update_date,
             mapyr.misc_yn01 as apc_group_ind,
             case
                 when mapyr.allow_contract_code_changes = 1 then 'Y'
                 else 'N'
             end as allow_contract_change_ind,
             case
                 when mapyr.calc_lock = 1 then 'Y'
                 else 'N'
             end as calc_lock_ind,
             mapyr.cers_profile_id,
             mapyr.calculation_no as calculation_num,
             mapyr.rate_schedule_name,
             mapyr.cers_term_id,
             mapyr.calc_base as calc_base_id,
             mapyr.interest_stop_date,
             mapyr.calc_base_survivor as calc_base_survivor_ind,
             mapyr.comp_method_choice,
             case
                 when mapyr.calc_result = 1 then 'Y'
                 else 'N'
             end as calc_result_ind,
             mapyr.calc_base_choice_resolved,
             mapyr.account_calc_situation,
             mapyr.mon_reason_id as reason_id,
             mapyr.mon_status_id as account_payer_status_id,
             mapyr.mon_project_id as project_id,
             mapyr.cob_method_id,
             mapyr.apl_appeal_id,
             mapyr.is_837_change_trigger as change_837_trigger_ind,
             mapyr.is_manual_trigger as manual_trigger_ind,
             case
                 when mapyr.is_elig_for_lifecycle_check = 1 then 'Y'
                 else 'N'
             end as life_check_eligible_ind,
             mapyr.external_appeal_code,
             mapyr.first_denial_date,
             coalesce(mapyr.coinsurance, 0) as coinsurance_amt,
             coalesce(mapyr.deductible, 0) as deductible_amt,
             coalesce(mapyr.copay, 0) as copay_amt,
             coalesce(mapyr.covered_charges_inst, 0) as covered_charges_inst_amt,
             coalesce(mapyr.total_expected_payment, 0) as total_exp_payment_amt,
             coalesce(mapyr.total_payments, 0) as total_payment_amt,
             coalesce(mapyr.total_pt_responsibility, 0) as total_pt_responsibility_amt,
             coalesce(mapyr.current_expected_payment, 0) as current_exp_pmt_amt,
             coalesce(mapyr.current_expected_contractual, 0) as current_exp_contractual_amt,
             coalesce(mapyr.total_adjustments, 0) as total_adjustment_amount,
             coalesce(mapyr.expected_contractual, 0) as total_expected_contractual_amt,
             coalesce(mapyr.misc_amt01, 0) as pa_prof_part_b_charge_amt,
             coalesce(mapyr.misc_amt02, 0) as pa_blood_deductible_amt,
             coalesce(mapyr.misc_amt03, 0) as auto_post_amt,
             coalesce(mapyr.covered_charges_prof, 0) as prof_covered_charges_amt,
             coalesce(mapyr.total_variance_adjustment, 0) as total_variance_adjustment,
             coalesce(mapyr.estimated_pt_responsibility, 0) as estimated_pt_responsibility,
             coalesce(mapyr.actual_pt_responsibility, 0) as actual_pt_responsibility,
             coalesce(mapyr.patient_accouting_coinsurance, 0) as pa_coinsurance_amt,
             coalesce(mapyr.total_denials, 0) as total_denial_amt,
             coalesce(mapyr.professional_expected_payment, 0) as prof_exp_payment_amt,
             coalesce(mapyr.state_tax_amt, 0) as seq_red_amt,
             mapyr.id as account_payor_id,
             mapyr.ppmc_ind as ppmc_ind,
             mpyr.pyr_type
      from {{ params.param_parallon_ra_stage_dataset_name }}.mon_account ma
      join {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure rccos on rccos.org_id = ma.org_id_provider
      and rccos.schema_id = ma.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer mapyr on mapyr.mon_account_id = ma.id
      and mapyr.schema_id = ma.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer mpyr on mpyr.id = mapyr.mon_payer_id
      and mpyr.schema_id = mapyr.schema_id
      left outer join {{ params.param_parallon_ra_stage_dataset_name }}.wq_org_account wqoa on wqoa.mon_account_payer_id = mapyr.id
      and wqoa.schema_id = mapyr.schema_id
      left outer join {{ params.param_parallon_ra_stage_dataset_name }}.wq_profile wqpf on wqpf.id = wqoa.wq_profile_id
      and wqpf.schema_id = wqoa.schema_id) a
   join {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys b on ----msk pbi 25190 change
 a.coid = b.coid
   and a.pat_acct_num = b.pat_acct_num
   join {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan c on -- --msk pbi 25190 change
 a.coid = c.coid
   and a.company_code = c.company_code
   and a.iplan_id = c.iplan_id) z ;

--  CALL dbadmin_procs.collect_stats_table('ra_edwra_staging','CC_Account_Payor_Merge_Stage' );

----MSK PBI 25190
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('ra_edwra_STAGING','CC_Account_Activity_Merge_Stage');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  Audit Addition --MSK PBI 25190
BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.mon_account_activity';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage';
SET audit_type= 'RECORD_COUNT';

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*)
   from
     (select rccos.company_code,rccos.coid, rccos.unit_num, mapyr.payer_rank as insurance_order_num, ma.account_no as pat_acct_num, case     when trim(mpyr.code) = 'NO INS' then 0     else cast(substr(trim(mpyr.code), 1, 3)|| substr(trim(mpyr.code), 5, 2) as integer) end as iplan_id, wqpf.name as wq_name, mapyr.authorization_code, mapyr.employer_name, mapyr.group_name as insurance_group_name, mapyr.insurance_group_no as insurance_group_num, mapyr.insured_name, mapyr.payer_identification_no as payer_identification_num, mapyr.rel_to_insured as rel_to_insured, mapyr.billed_date, mapyr.date_created, mapyr.date_updated, mapyr.billing_name, mapyr.billing_address1, mapyr.billing_address2, mapyr.billing_city, mapyr.billing_state, trim(mapyr.billing_zip) as billing_zip, trim(mapyr.billing_phone) as billing_phone, mapyr.billing_contact_person, trim(mapyr.billing_fax) as billing_fax, mapyr.billing_contact_email, mapyr.rel_to_insured as relation_to_insured, mapyr.calculation_date, case     when mapyr.is_eligible = 1 then 'Y'     else 'N' end as is_eligible_ind, mapyr.insureds_gender as insured_gender, mapyr.insureds_date_of_birth as insured_date_of_birth, mapyr.misc_char01 as pa_financial_class_code, mapyr.misc_char02 as drg_version_code, mapyr.misc_char03 as pa_auto_post_ind, mapyr.misc_char04 as bill_reason_code, mapyr.misc_date01 as pa_denial_update_date, mapyr.misc_yn01 as apc_group_ind, case     when mapyr.allow_contract_code_changes = 1 then 'Y'     else 'N' end as allow_contract_change_ind, case     when mapyr.calc_lock = 1 then 'Y'     else 'N' end as calc_lock_ind, mapyr.cers_profile_id, mapyr.calculation_no as calculation_num, mapyr.rate_schedule_name, mapyr.cers_term_id, mapyr.calc_base as calc_base_id, mapyr.interest_stop_date, mapyr.calc_base_survivor as calc_base_survivor_ind, mapyr.comp_method_choice, case     when mapyr.calc_result = 1 then 'Y'     else 'N' end as calc_result_ind, mapyr.calc_base_choice_resolved, mapyr.account_calc_situation, mapyr.mon_reason_id as reason_id, mapyr.mon_status_id as account_payer_status_id, mapyr.mon_project_id as project_id, mapyr.cob_method_id, mapyr.apl_appeal_id, mapyr.is_837_change_trigger as change_837_trigger_ind, mapyr.is_manual_trigger as manual_trigger_ind, case     when mapyr.is_elig_for_lifecycle_check = 1 then 'Y'     else 'N' end as life_check_eligible_ind, mapyr.external_appeal_code, mapyr.first_denial_date, coalesce(mapyr.coinsurance, 0) as coinsurance_amt, coalesce(mapyr.deductible, 0) as deductible_amt, coalesce(mapyr.copay, 0) as copay_amt, coalesce(mapyr.covered_charges_inst, 0) as covered_charges_inst_amt, coalesce(mapyr.total_expected_payment, 0) as total_exp_payment_amt, coalesce(mapyr.total_payments, 0) as total_payment_amt, coalesce(mapyr.total_pt_responsibility, 0) as total_pt_responsibility_amt, coalesce(mapyr.current_expected_payment, 0) as current_exp_pmt_amt, coalesce(mapyr.current_expected_contractual, 0) as current_exp_contractual_amt, coalesce(mapyr.total_adjustments, 0) as total_adjustment_amount, coalesce(mapyr.expected_contractual, 0) as total_expected_contractual_amt, coalesce(mapyr.misc_amt01, 0) as pa_prof_part_b_charge_amt, coalesce(mapyr.misc_amt02, 0) as pa_blood_deductible_amt, coalesce(mapyr.misc_amt03, 0) as auto_post_amt, coalesce(mapyr.covered_charges_prof, 0) as prof_covered_charges_amt, coalesce(mapyr.total_variance_adjustment, 0) as total_variance_adjustment, coalesce(mapyr.estimated_pt_responsibility, 0) as estimated_pt_responsibility, coalesce(mapyr.actual_pt_responsibility, 0) as actual_pt_responsibility, coalesce(mapyr.patient_accouting_coinsurance, 0) as pa_coinsurance_amt, coalesce(mapyr.total_denials, 0) as total_denial_amt, coalesce(mapyr.professional_expected_payment, 0) as prof_exp_payment_amt, coalesce(mapyr.state_tax_amt, 0) as seq_red_amt, mapyr.id as account_payor_id, mapyr.ppmc_ind as ppmc_ind, mpyr.pyr_type
      from {{ params.param_parallon_ra_stage_dataset_name }}.mon_account ma
      join {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure rccos on rccos.org_id = ma.org_id_provider
      and rccos.schema_id = ma.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer mapyr on mapyr.mon_account_id = ma.id
      and mapyr.schema_id = ma.schema_id
      join {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer mpyr on mpyr.id = mapyr.mon_payer_id
      and mpyr.schema_id = mapyr.schema_id
      left outer join {{ params.param_parallon_ra_stage_dataset_name }}.wq_org_account wqoa on wqoa.mon_account_payer_id = mapyr.id
      and wqoa.schema_id = mapyr.schema_id
      left outer join {{ params.param_parallon_ra_stage_dataset_name }}.wq_profile wqpf on wqpf.id = wqoa.wq_profile_id
      and wqpf.schema_id = wqoa.schema_id) a
   join {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys b on ----msk pbi 25190 change
 a.coid = b.coid
   and a.pat_acct_num = b.pat_acct_num
   join {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan c on -- --msk pbi 25190 change
 a.coid = c.coid
   and a.company_code = c.company_code
   and a.iplan_id = c.iplan_id
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage
);

SET difference = 
CASE WHEN expected_value <> 0 Then CAST(((ABS(actual_value - expected_value)/expected_value) * 100) AS INT64) 
     WHEN expected_value = 0 and actual_value = 0 Then 0 
	 ELSE actual_value
END;

SET audit_status = CASE WHEN difference <= 0 THEN "PASS" ELSE "FAIL" END;

INSERT INTO {{ params.param_parallon_ra_audit_dataset_name }}.audit_control
(uuid, table_id, src_sys_nm, src_tbl_nm, tgt_tbl_nm, audit_type, 
expected_value, actual_value, load_start_time, load_end_time, 
load_run_time, job_name, audit_time, audit_status)
VALUES
(GENERATE_UUID(), cast(srctableid as int64), 'ra',srctablename, tgttablename, audit_type,
expected_value, actual_value, cast(tableload_start_time as datetime), cast(tableload_end_time AS datetime),
tableload_run_time, job_name, audit_time, audit_status
);

-- INSERT INTO EDWRA_AC.ETL_Control_Expected (Job_Name, Job_Start_Date_Time, Control_ID, Control_Tolerance_Percent, Control_Tolerance_Amt, Control_Value_Expected)
-- SELECT Y.Job_Name,
       -- Y.Job_Start_Date_Time,
       -- 1 AS Control_ID,
       -- 0 AS Control_Tolerance_Percent,
       -- 0 AS Control_Tolerance_Amt,
       -- COUNT(ZEROIFNULL(X.Patient_DW_Id)) AS Control_Value_Expected
-- FROM
  -- (SELECT Job_Name,
          -- Job_Start_Date_Time
   -- FROM EDWRA_AC.ETL_JOB_RUN
   -- WHERE Job_Name = 'CTDRA262'
     -- AND Job_Start_Date_Time =
       -- (SELECT MAX(jr2.Job_Start_Date_Time)
        -- FROM EDWRA_AC.ETL_Job_Run jr2
        -- WHERE jr2.Job_Name = 'CTDRA262'
          -- AND jr2.Job_End_Date_Time IS NULL ) ) Y,

  -- (SELECT Patient_DW_ID,
          -- Payor_DW_ID,
          -- Company_Code,
          -- Coid,
          -- Unit_Num
   -- FROM EDWRA_STAGING.CC_Account_Payor_Merge_Stage) X
-- GROUP BY 1,
         -- 2,
         -- 3,
         -- 4,
         -- 5;

-- .IF ERRORCODE <> 0 THEN.QUIT ERRORCODE;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- MSK PBI 25190
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;

merge into {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor x using
  (select *
   from {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage) z on (x.patient_dw_id = z.patient_dw_id
      and x.payor_dw_id = z.payor_dw_id
      and x.iplan_order_num = z.iplan_order_num) when matched then
update
set company_code = z. company_code ,
    coid = z. coid ,
    unit_num = z. unit_num ,
    pat_acct_num = z. pat_acct_num ,
    iplan_id = z. iplan_id ,
    work_queue_name = z. work_queue_name ,
    rate_schedule_name = z. rate_schedule_name ,
    employer_name = z. employer_name ,
    insurance_group_name = z. insurance_group_name ,
    insurance_group_num_cd = z. insurance_group_num_cd ,
    insured_name = z. insured_name ,
    insured_gender_cd = z. insured_gender_cd ,
    insured_birth_date = z. insured_birth_date ,
    payor_identification_num_cd = z. payor_identification_num_cd ,
    authorization_cd = z. authorization_cd ,
    billed_date = z. billed_date ,
    billing_name = z. billing_name ,
    billing_address_1 = z. billing_address_1 ,
    billing_address_2 = z. billing_address_2 ,
    billing_city_name = z. billing_city_name ,
    billing_state_code = z. billing_state_code ,
    billing_zip_code = z. billing_zip_code ,
    billing_phone_num_cd = z. billing_phone_num_cd ,
    billing_fax_num = z. billing_fax_num ,
    billing_contact_person_name = z. billing_contact_person_name ,
    billing_contact_email_name = z. billing_contact_email_name ,
    relation_to_insured_pat_cd = z. relation_to_insured_pat_cd ,
    drg_version = z. drg_version ,
    pa_denial_date = z. pa_denial_date ,
    first_denial_date = z. first_denial_date ,
    interest_stop_date = z. interest_stop_date ,
    calc_date = z. calc_date ,
    calc_num = z. calc_num ,
    calc_result_ind = z. calc_result_ind ,
    calc_base_choice_resolved_ind = z. calc_base_choice_resolved_ind ,
    calc_lock_ind = z. calc_lock_ind ,
    pa_autm_post_ind = z. pa_autm_post_ind ,
    apc_group_ind = z. apc_group_ind ,
    eligible_ind = z. eligible_ind ,
    account_calc_situation_ind = z. account_calc_situation_ind ,
    allow_contract_code_change_ind = z. allow_contract_code_change_ind ,
    change_claim_trigger_ind = z. change_claim_trigger_ind ,
    manual_trigger_ind = z. manual_trigger_ind ,
    life_check_eligible_ind = z. life_check_eligible_ind ,
    pa_financial_class_code = z. pa_financial_class_code ,
    bill_reason_code = z. bill_reason_code ,
    external_appeal_code = z. external_appeal_code ,
    total_exp_payment_amt = z. total_exp_payment_amt ,
    total_payment_amt = z. total_payment_amt ,
    total_denial_amt = z. total_denial_amt ,
    total_pat_responsibility_amt = z. total_pat_responsibility_amt ,
    est_pat_responsibility_amt = z. est_pat_responsibility_amt ,
    actual_pat_responsibility_amt = z. actual_pat_responsibility_amt ,
    total_exp_contractual_amt = z. total_exp_contractual_amt ,
    total_adjustment_amt = z. total_adjustment_amt ,
    total_variance_adjustment_amt = z. total_variance_adjustment_amt ,
    current_exp_payment_amt = z. current_exp_payment_amt ,
    current_exp_contractual_amt = z. current_exp_contractual_amt ,
    prof_covered_charge_amt = z. prof_covered_charge_amt ,
    prof_exp_payment_amt = z. prof_exp_payment_amt ,
    pa_prof_part_b_charge_amt = z. pa_prof_part_b_charge_amt ,
    pa_blood_deductible_amt = z. pa_blood_deductible_amt ,
    sqstrtn_reduction_amt = z. sqstrtn_reduction_amt ,
    autm_post_amt = z. autm_post_amt ,
    pa_coinsurance_amt = z. pa_coinsurance_amt ,
    coinsurance_amt = z. coinsurance_amt ,
    deductible_amt = z. deductible_amt ,
    copay_amt = z. copay_amt ,
    covered_charge_instu_amt = z. covered_charge_instu_amt ,
    cmpt_method_choice_id = z. cmpt_method_choice_id ,
    calc_base_id = z. calc_base_id ,
    calc_base_survivor_id = z. calc_base_survivor_id ,
    reason_id = z. reason_id ,
    account_payor_status_id = z. account_payor_status_id ,
    appeal_id = z. appeal_id ,
    cers_profile_id = z. cers_profile_id ,
    cers_term_id = z. cers_term_id ,
    project_id = z. project_id ,
    creation_date = z. creation_date ,
    update_date = z. update_date ,
    account_payor_id = z. account_payor_id ,
    prepay_mgd_care_sw = z. prepay_mgd_care_sw ,
    capping_method_id = z. capping_method_id ,
    payer_type_code = z.payer_type_code,
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) when not matched then
insert (account_calc_situation_ind,
        account_payor_id,
        account_payor_status_id,
        actual_pat_responsibility_amt,
        allow_contract_code_change_ind,
        apc_group_ind,
        appeal_id,
        authorization_cd,
        autm_post_amt,
        bill_reason_code,
        billed_date,
        billing_address_1,
        billing_address_2,
        billing_city_name,
        billing_contact_email_name,
        billing_contact_person_name,
        billing_fax_num,
        billing_name,
        billing_phone_num_cd,
        billing_state_code,
        billing_zip_code,
        calc_base_choice_resolved_ind,
        calc_base_id,
        calc_base_survivor_id,
        calc_date,
        calc_lock_ind,
        calc_num,
        calc_result_ind,
        cers_profile_id,
        cers_term_id,
        change_claim_trigger_ind,
        cmpt_method_choice_id,
        coid,
        coinsurance_amt,
        company_code,
        copay_amt,
        covered_charge_instu_amt,
        creation_date,
        current_exp_contractual_amt,
        current_exp_payment_amt,
        deductible_amt,
        drg_version,
        dw_last_update_date_time,
        eligible_ind,
        employer_name,
        est_pat_responsibility_amt,
        external_appeal_code,
        first_denial_date,
        insurance_group_name,
        insurance_group_num_cd,
        insured_birth_date,
        insured_gender_cd,
        insured_name,
        interest_stop_date,
        iplan_id,
        iplan_order_num,
        life_check_eligible_ind,
        manual_trigger_ind,
        pa_autm_post_ind,
        pa_blood_deductible_amt,
        pa_coinsurance_amt,
        pa_denial_date,
        pa_financial_class_code,
        pa_prof_part_b_charge_amt,
        pat_acct_num,
        patient_dw_id,
        payor_dw_id,
        payor_identification_num_cd,
        prof_covered_charge_amt,
        prof_exp_payment_amt,
        project_id,
        rate_schedule_name,
        reason_id,
        relation_to_insured_pat_cd,
        source_system_code,
        sqstrtn_reduction_amt,
        total_adjustment_amt,
        total_denial_amt,
        total_exp_contractual_amt,
        total_exp_payment_amt,
        total_pat_responsibility_amt,
        total_payment_amt,
        total_variance_adjustment_amt,
        unit_num,
        update_date,
        work_queue_name,
        capping_method_id,
        prepay_mgd_care_sw,
        payer_type_code)
values (z.account_calc_situation_ind,z. account_payor_id,z. account_payor_status_id,z. actual_pat_responsibility_amt,z. allow_contract_code_change_ind,z. apc_group_ind,z. appeal_id,z. authorization_cd,z. autm_post_amt,z. bill_reason_code,z. billed_date,z. billing_address_1,z. billing_address_2,z. billing_city_name,z. billing_contact_email_name,z. billing_contact_person_name,z. billing_fax_num,z. billing_name,z. billing_phone_num_cd,z. billing_state_code,z. billing_zip_code,z. calc_base_choice_resolved_ind,z. calc_base_id,z. calc_base_survivor_id,z. calc_date,z. calc_lock_ind,z. calc_num,z. calc_result_ind,z. cers_profile_id,z. cers_term_id,z. change_claim_trigger_ind,z. cmpt_method_choice_id,z. coid,z. coinsurance_amt,z. company_code,z. copay_amt,z. covered_charge_instu_amt,z. creation_date,z. current_exp_contractual_amt,z. current_exp_payment_amt,z. deductible_amt,z. drg_version,z. dw_last_update_date_time,z. eligible_ind,z. employer_name,z. est_pat_responsibility_amt,z. external_appeal_code,z. first_denial_date,z. insurance_group_name,z. insurance_group_num_cd,z. insured_birth_date,z. insured_gender_cd,z. insured_name,z. interest_stop_date,z. iplan_id,z. iplan_order_num,z. life_check_eligible_ind,z. manual_trigger_ind,z. pa_autm_post_ind,z. pa_blood_deductible_amt,z. pa_coinsurance_amt,z. pa_denial_date,z. pa_financial_class_code,z. pa_prof_part_b_charge_amt,z. pat_acct_num,z. patient_dw_id,z. payor_dw_id,z. payor_identification_num_cd,z. prof_covered_charge_amt,z. prof_exp_payment_amt,z. project_id,z. rate_schedule_name,z. reason_id,z. relation_to_insured_pat_cd,z. source_system_code,z. sqstrtn_reduction_amt,z. total_adjustment_amt,z. total_denial_amt,z. total_exp_contractual_amt,z. total_exp_payment_amt,z. total_pat_responsibility_amt,z. total_payment_amt,z. total_variance_adjustment_amt,z. unit_num,z. update_date,z. work_queue_name,z. capping_method_id,z. prepay_mgd_care_sw, z.payer_type_code);

SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id ,payor_dw_id , iplan_order_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor
      GROUP BY patient_dw_id ,payor_dw_id , iplan_order_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  CALL dbadmin_procs.collect_stats_table('edwra','CC_Account_Activity');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_account_payor_merge_stage;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;