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

SET srctablename = concat(srcdataset_id, '.','cc_account_payor_merge_stage' ); -- This needs to be added

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
   and a.iplan_id = c.iplan_id)
) a
);

SET act_values_list =
(
SELECT [format('%20d', a.row_count)]
FROM
  (SELECT count(*) AS ROW_COUNT
   FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_account_payor
      WHERE cc_account_payor.dw_last_update_date_time >=
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