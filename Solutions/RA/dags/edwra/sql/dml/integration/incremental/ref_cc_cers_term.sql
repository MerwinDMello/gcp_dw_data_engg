DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-01T19:21:22.661879Z
-- Translation job ID: 55abbc96-797c-4732-9cf9-73ba435f68fb
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/ehYakm/input/ref_cc_cers_term.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/************************************************************************************************
     Developer: Sean Wilson
          Date: 7/7/2014
          Name: Ref_CC_CERS_Term.sql
	  Mod1: Update the sql to correct when match set Ref_Cers_Profile_Id = Z.Ref_Cers_Term_Id
	        condition. 10/20/2014. AP.
	  Mod2: Updated script to match PDM on 11/11/2014 SW.
	  Mod3: Updated script to include new column, LOA_Profile_Desc, for Contract Modeling on 7/15/2015 JC.
	  Mod4: Add purge statement at the end to clean-up deleted rows in source.  -  09/17/2015  jac
	  Mod5: CR152 - ICD10 - Changed column from ICD_Version_Id to ICD_Version_Desc -  09/30/2015  jac
	  Mod6: PBI16245 - Duplication fixed for unique index.  Changed join on calc_profile to only get
	        prfl_type_id = 3000051 for LOA Profiles rows in attribute table. 4/3/2018 SW.
	Mod7:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod:8 PBI 28631 to add new columns
**************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA258;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_term AS x USING
  (SELECT rccos.company_code,
          rccos.coid,
          ctrm.id AS ref_cers_term_id,
          ctrm.cers_profile_id AS ref_cers_profile_id,
          ctrm.effective_date_method_id,
          ctrm.cers_profile_id_comparison AS cers_profile_comparison_id,
          ctrm.comparison_calc_method_id,
          ctrm.ipf_transition_calc_method AS ipf_transition_calc_method_id,
          ctrm.inpatient_reimb_method_id,
          ctrm.outpatient_reimb_method_id,
          ctrm.user_id_owner AS owner_user_id,
          ctrm.asc_ranking_method_id,
          ctrm.validated_by AS validated_by_user_id,
          ctrm.drg_calculation_method_id,
          ctrm.gvt_capital_method_id,
          ctrm.ce_apc_profile_id,
          ctrm.ce_profile_fee_schedule_id,
          ctrm.status_id,
          ctrm.cers_parent_term_id,
          ctrm.irf_reimb_method_id,
          ctrm.ce_irf_profile_id,
          ctrm.ce_ipf_profile_id,
          ctrm.drg_transfer_profile_id,
          ctrm.snf_reimb_method_id,
          ctrm.calc_comp_method_set_by_user AS comparison_calc_method_user_id,
          ctrm.cob_method_id,
          ctrm.apc_code_source AS apc_code_source_id,
          ctrm.comparison_calc_level AS comparison_calc_level_id,
          ctrm.gvt_newtech_profile_id,
          ctrm.ce_coins_fs_profile_id,
          ctrm.ce_snf_profile_id,
          ctrm.ce_state_tax_profile_id,
          ctrm.ce_hierarchy_profile_id,
          ctrm.inpatient_accrual_method_id,
          ctrm.outpatient_accrual_method_id,
          ctrm.ce_tricare_emer_rm_profile_id AS ce_tri_er_profile_id,
          ctrm.ce_tricare_apc_profile_id AS ce_tri_apc_profile_id,
          ctrm.ce_atp_profile_id,
          ctrm.ce_tri_drg_code_profile_id,
          ctrm.coins_calc_perc_selection AS coins_calc_perc_sel_id,
          ctrm.gvt_ime_component_id,
          ctrm.cost_share_calc_perc_selection AS cst_share_calc_perc_sel_id,
          CASE
              WHEN ctrm.is_cob_capped = 1 THEN 'Y'
              ELSE 'N'
          END AS is_cob_capped_ind,
          CASE
              WHEN ctrm.is_model = 1 THEN 'Y'
              ELSE 'N'
          END AS is_model_ind,
          CASE
              WHEN ctrm.is_validated = 1 THEN 'Y'
              ELSE 'N'
          END AS is_validated_ind,
          CASE
              WHEN ctrm.generate_contractuals = 1 THEN 'Y'
              ELSE 'N'
          END AS generate_contractual_ind,
          CASE
              WHEN ctrm.generate_account_calc_detail = 1 THEN 'Y'
              ELSE 'N'
          END AS generate_acct_calc_detail_ind,
          CASE
              WHEN ctrm.drg_breakout_transfer = 1 THEN 'Y'
              ELSE 'N'
          END AS drg_breakout_transfer_ind,
          CASE
              WHEN ctrm.drg_breakout_expired = 1 THEN 'Y'
              ELSE 'N'
          END AS drg_breakout_expired_ind,
          CASE
              WHEN ctrm.drg_breakout_against_medadvice = 1 THEN 'Y'
              ELSE 'N'
          END AS drg_breakout_ama_ind,
          CASE
              WHEN ctrm.gvt_incl_dsh_ime_to_cost_outl = 1 THEN 'Y'
              ELSE 'N'
          END AS gvt_dsh_ime_cst_outlr_ind,
          CASE
              WHEN ctrm.gvt_include_scp = 1 THEN 'Y'
              ELSE 'N'
          END AS gvt_include_scp_ind,
          CASE
              WHEN ctrm.ipf_include_fsc_in_outlier = 1 THEN 'Y'
              ELSE 'N'
          END AS ipf_inc_fsc_in_outlier_ind,
          CASE
              WHEN ctrm.ipf_include_fsa_in_outlier = 1 THEN 'Y'
              ELSE 'N'
          END AS ipf_inc_fsa_in_outlier_ind,
          CASE
              WHEN ctrm.apc_exclude_outlier = 1 THEN 'Y'
              ELSE 'N'
          END AS apc_exclude_outlier_ind,
          CASE
              WHEN ctrm.include_gaf = 1 THEN 'Y'
              ELSE 'N'
          END AS include_gaf_ind,
          CASE
              WHEN ctrm.irf_is_rural_facility = 1 THEN 'Y'
              ELSE 'N'
          END AS irf_is_rural_facility_ind,
          CASE
              WHEN ctrm.irf_is_teaching_facility = 1 THEN 'Y'
              ELSE 'N'
          END AS irf_is_teaching_facility_ind,
          CASE
              WHEN ctrm.tricare_apc_is_sole_comm_hosp = 1 THEN 'Y'
              ELSE 'N'
          END AS tri_apc_is_sole_comm_hosp_ind,
          CASE
              WHEN ctrm.apc_is_rural_sole_comm_hosp = 1 THEN 'Y'
              ELSE 'N'
          END AS apc_is_rural_sole_comm_hsp_ind,
          CASE
              WHEN ctrm.gvt_is_transfer_excluded = 1 THEN 'Y'
              ELSE 'N'
          END AS gvt_is_transfer_excluded_ind,
          CASE
              WHEN ctrm.gvt_is_outlier_excluded = 1 THEN 'Y'
              ELSE 'N'
          END AS gvt_is_outlier_excluded_ind,
          CASE
              WHEN ctrm.gvt_is_disregard_subterm = 1 THEN 'Y'
              ELSE 'N'
          END AS gvt_is_disregard_subterm_ind,
          CASE
              WHEN ctrm.is_valuated = 1 THEN 'Y'
              ELSE 'N'
          END AS is_valuated_ind,
          CASE
              WHEN ctrm.is_copay = 1 THEN 'Y'
              ELSE 'N'
          END AS is_copay_ind,
          CASE
              WHEN ctrm.is_pt_resp_used_for_cob = 1 THEN 'Y'
              ELSE 'N'
          END AS is_pat_resp_used_for_cob_ind,
          CASE
              WHEN ctrm.apc_rural_hosp_ind = 1 THEN 'Y'
              ELSE 'N'
          END AS apc_rural_hospital_ind,
          CASE
              WHEN ctrm.is_xfer_drg_capped_to_std_rate = 1 THEN 'Y'
              ELSE 'N'
          END AS xfer_drg_capped_std_rate_ind,
          ctrm.effective_date_begin AS effective_begin_date,
          ctrm.effective_date_end AS effective_end_date,
          ctrm.date_validated AS validated_date,
          ctrm.date_created AS cers_term_create_date,
          ctrm.date_updated AS cers_term_update_date,
          ctrm.drg_version,
          ctrm.drg_type,
          ctrm.comparison_calc_modifier AS comparison_calc_modifier_amt,
          ctrm.gvt_base_rate AS gvt_base_rate_amt,
          ctrm.gvt_labor_rate AS gvt_labor_rate_amt,
          ctrm.gvt_wage_index AS gvt_wage_index_amt,
          ctrm.gvt_nonlabor_index AS gvt_nonlabor_index_amt,
          ctrm.gvt_capital_federal_rate AS gvt_capital_federal_rate_amt,
          ctrm.gvt_geographical_factor AS gvt_geographical_factor_qty,
          ctrm.gvt_urban_add_on_factor AS gvt_urban_add_on_factor_qty,
          ctrm.gvt_cost_of_liv_addon_factor AS gvt_cst_liv_addon_factor_qty,
          ctrm.gvt_disp_share_oper_adj AS gvt_disp_share_oper_adj_amt,
          ctrm.gvt_disp_share_capital_adj AS gvt_disp_share_capital_adj_amt,
          ctrm.gvt_ind_med_edu_oper_adj AS gvt_ind_med_edu_oper_adj_amt,
          ctrm.gvt_ind_med_edu_capital_adj AS gvt_ind_med_edu_cap_adj_amt,
          ctrm.gvt_oper_cost_charge_ratio AS gvt_oper_cst_charge_ratio_qty,
          ctrm.gvt_capital_cost_charge_ratio AS gvt_cap_cst_charge_ratio_qty,
          ctrm.gvt_drg_threshold_adj AS gvt_drg_threshold_adj_amt,
          ctrm.gvt_labor_share AS gvt_labor_share_amt,
          ctrm.gvt_dispropor_share_reduc_adj AS gvt_disprpr_share_red_adj_amt,
          ctrm.gvt_oper_cost_of_liv_adj AS gvt_oper_cost_of_livng_adj_amt,
          ctrm.apc_federal_rate AS apc_federal_rate_amt,
          ctrm.apc_wage_index AS apc_wage_index_amt,
          ctrm.apc_labor_percent AS apc_labor_percent_amt,
          ctrm.apc_cost_charge_ratio AS apc_cost_charge_ratio_amt,
          ctrm.apc_cost_threshold AS apc_cost_threshold_amt,
          coalesce(ctrm.apc_cost_outlier_percent, CAST(0 AS NUMERIC)) AS apc_cost_outlier_percent_amt,
          ctrm.apc_pro_rata_adjustment AS apc_pro_rata_adj_amt,
          ctrm.apc_coinsurance_limit AS apc_coinsurance_limit_amt,
          ctrm.apc_deductible AS apc_deductible_amt,
          ctrm.gvt_scp_rate AS gvt_scp_rate_amt,
          ctrm.apc_fixed_threshold AS apc_fixed_threshold_amt,
          ctrm.ipf_labor_share_amount AS ipf_labor_share_amt,
          ctrm.ipf_nonlabor_share_amount AS ipf_nonlabor_share_amt,
          ctrm.ipf_labor_share_percent AS ipf_labor_share_percent_amt,
          ctrm.ipf_wage_index AS ipf_wage_index_amt,
          ctrm.ipf_teaching_adj_factor AS ipf_teaching_adj_factor_amt,
          ctrm.ipf_cola_adj_factor AS ipf_cola_adj_factor_amt,
          ctrm.ipf_fixed_loss_amount AS ipf_fixed_loss_amt,
          ctrm.ipf_cost_charge_ratio AS ipf_cost_charge_ratio_amt,
          ctrm.ipf_tefra_amount AS ipf_tefra_amt,
          ctrm.ipf_pps_federal_percent AS ipf_pps_federal_percent_amt,
          ctrm.ipf_tefra_hospital_percent AS ipf_tefra_hospital_percent_amt,
          ctrm.gvt_noscaasotc_adjustment AS gvt_noscaasotc_adj_amt,
          ctrm.irf_std_payment_conv_factor AS irf_std_pmt_conv_factor_amt,
          ctrm.irf_labor_share AS irf_labor_share_amt,
          ctrm.irf_cbsa_wage_index AS irf_cbsa_wage_index_amt,
          ctrm.irf_lip_adjustment AS irf_lip_adj_amt,
          ctrm.irf_cost_to_charge_ratio AS irf_cost_to_charge_ratio_amt,
          ctrm.irf_adj_threshold_amt AS irf_adj_threshold_amt,
          ctrm.irf_teaching_adjustment AS irf_teaching_adj_amt,
          ctrm.snf_cbsa_wage_index AS snf_cbsa_wage_index_amt,
          ctrm.snf_icd9_042_multiplier AS snf_icd9_042_multiplier_amt,
          ctrm.xfer_add_los AS xfer_add_los_qty,
          ctrm.inpatient_accrual_value AS inpatient_accrual_value_amt,
          ctrm.outpatient_accrual_value AS outpatient_accrual_value_amt,
          ctrm.apc_bilateral_disc_fraction AS apc_bilateral_disc_fract_amt,
          ctrm.apc_rural_sch_adjustment AS apc_rural_sch_adj_amt,
          ctrm.apc_terminated_disc_fraction AS apc_terminated_disc_fract_amt,
          ctrm.irf_outlier_percent AS irf_outlier_percent_amt,
          ctrm.tri_asa_labor_rel_portion AS tri_asa_labor_rel_portn_amt,
          ctrm.tri_asa_nonlabor_rel_portion AS tri_asa_nonlabor_rel_portn_amt,
          ctrm.tri_wage_index AS tri_wage_index_amt,
          ctrm.tri_idme_adjustment_factor AS tri_idme_adj_factor_amt,
          ctrm.tri_short_stay_mar_cost_factor AS tri_shortstay_mar_cst_fctr_amt,
          ctrm.tri_er_wage_index AS tri_er_wage_index_amt,
          ctrm.tri_er_labor_portion AS tri_er_labor_portion_amt,
          ctrm.tri_er_nonlabor_portion AS tri_er_nonlabor_portion_amt,
          ctrm.tri_er_cost_share_percent AS tri_er_cost_share_percent_amt,
          ctrm.tri_cost_to_charge_ratio AS tri_cost_to_chrg_ratio_amt,
          ctrm.tri_noscastc AS tri_noscastic_amt,
          ctrm.tri_labor_share_percent AS tri_labor_share_percent_amt,
          ctrm.tri_nonlabor_share_percent AS tri_nonlabor_share_percent_amt,
          ctrm.tricare_apc_wage_index AS tri_apc_wage_index_amt,
          ctrm.tricare_apc_cost_charge_ratio AS tri_apc_cost_chrg_ratio_amt,
          ctrm.tricare_apc_sch_adjustment AS tri_apc_sch_adj_amt,
          ctrm.tri_neonate_factor AS tri_neonate_factor_amt,
          ctrm.gvt_sub_term_outlier_percent AS gvt_subterm_outlier_pct_amt,
          ctrm.gvt_readm_adj_fctr AS gvt_readm_adj_factor_amt,
          ctrm.gvt_vbj_adj_fctr AS gvt_vbj_adj_factor_amt,
          ctrm.dsh_uncmps_care_addon AS dsh_uncmps_care_addon_amt,
          ctrm.implant_ccr AS implant_ccr_amt,
          ctrm.low_vol_adj_fctr AS low_vol_adj_fctr_amt,
          ctrm.mdh_amt AS mdh_amt,
          ctrm.irf_rural_adjustment AS irf_rural_adj_fctr_amt,
          ctrm.ipf_rural_adj_factor AS ipf_rural_adj_fctr_amt,
          ctrm.copay_amt,
          ctrm.calc_base_choice_set_by_user AS calc_base_choice_set_by_usr,
          ctrm.drg_profile_id,
          pvicd.display_text AS icd_version_desc,
          ctrm.dsh_uncmps_care_addon,
          concat(clpr.name, ' - ', pvloapr.display_text) AS loa_profile,
          cte_event.event_date AS cers_term_activated_date_time,
          ctrm.user_id_updater AS cers_term_updated_by_user_id
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_term AS ctrm
   LEFT OUTER JOIN
     (SELECT calc_prfl_cers_term_assn.schema_id,
             calc_prfl_cers_term_assn.cers_term_id,
             calc_prfl_cers_term_assn.calc_prfl_id,
             rank() OVER (PARTITION BY calc_prfl_cers_term_assn.schema_id,
                                       calc_prfl_cers_term_assn.cers_term_id
                          ORDER BY calc_prfl_cers_term_assn.calc_prfl_id DESC) AS ranking
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.calc_prfl_cers_term_assn) AS pta ON pta.cers_term_id = ctrm.id
   AND pta.schema_id = ctrm.schema_id
   AND pta.ranking = 1
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.calc_prfl AS clpr ON clpr.calc_prfl_id = pta.calc_prfl_id
   AND clpr.schema_id = pta.schema_id
   AND clpr.prfl_type_id = 3000051
   LEFT OUTER JOIN --  LOA Profiles for associated exclusion ids.
 {{ params.param_parallon_ra_stage_dataset_name }}.calc_prfl_attr AS patr ON patr.calc_prfl_id = clpr.calc_prfl_id
   AND patr.schema_id = clpr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvloapr ON pvloapr.id = patr.excl_method_id
   AND pvloapr.schema_id = patr.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvicd ON pvicd.id = ctrm.icd_version_id
   AND pvicd.schema_id = ctrm.schema_id
   INNER JOIN {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.schema_id = ctrm.schema_id
   LEFT OUTER JOIN
     (SELECT cte.schema_id,
             cte.cers_term_id,
             max(cte.event_date) AS event_date
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cers_term_event AS cte
      WHERE cte.event_type = 2666429
      GROUP BY 1,
               2) AS cte_event ON cte_event.cers_term_id = ctrm.id
   AND cte_event.schema_id = ctrm.schema_id) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.cers_term_id = z.ref_cers_term_id WHEN MATCHED THEN
UPDATE
SET cers_profile_id = ROUND(z.ref_cers_profile_id, 0, 'ROUND_HALF_EVEN'),
    effective_date_method_id = ROUND(z.effective_date_method_id, 0, 'ROUND_HALF_EVEN'),
    cers_profile_comparison_id = ROUND(z.cers_profile_comparison_id, 0, 'ROUND_HALF_EVEN'),
    comparison_calc_method_id = ROUND(z.comparison_calc_method_id, 0, 'ROUND_HALF_EVEN'),
    ipf_transition_calc_method_id = ROUND(z.ipf_transition_calc_method_id, 0, 'ROUND_HALF_EVEN'),
    inpatient_reimb_method_id = ROUND(z.inpatient_reimb_method_id, 0, 'ROUND_HALF_EVEN'),
    outpatient_reimb_method_id = ROUND(z.outpatient_reimb_method_id, 0, 'ROUND_HALF_EVEN'),
    owner_user_id = ROUND(z.owner_user_id, 0, 'ROUND_HALF_EVEN'),
    asc_ranking_method_id = ROUND(z.asc_ranking_method_id, 0, 'ROUND_HALF_EVEN'),
    validated_by_user_id = ROUND(z.validated_by_user_id, 0, 'ROUND_HALF_EVEN'),
    drg_calc_method_id = ROUND(z.drg_calculation_method_id, 0, 'ROUND_HALF_EVEN'),
    gov_capital_method_id = ROUND(z.gvt_capital_method_id, 0, 'ROUND_HALF_EVEN'),
    ce_apc_profile_id = ROUND(z.ce_apc_profile_id, 0, 'ROUND_HALF_EVEN'),
    status_id = ROUND(z.status_id, 0, 'ROUND_HALF_EVEN'),
    ce_profile_fee_schedule_id = ROUND(z.ce_profile_fee_schedule_id, 0, 'ROUND_HALF_EVEN'),
    cers_parent_term_id = ROUND(z.cers_parent_term_id, 0, 'ROUND_HALF_EVEN'),
    irf_reimb_method_id = ROUND(z.irf_reimb_method_id, 0, 'ROUND_HALF_EVEN'),
    ce_irf_profile_id = ROUND(z.ce_irf_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_ipf_profile_id = ROUND(z.ce_ipf_profile_id, 0, 'ROUND_HALF_EVEN'),
    drg_transfer_profile_id = ROUND(z.drg_transfer_profile_id, 0, 'ROUND_HALF_EVEN'),
    snf_reimb_method_id = ROUND(z.snf_reimb_method_id, 0, 'ROUND_HALF_EVEN'),
    cob_method_id = ROUND(z.cob_method_id, 0, 'ROUND_HALF_EVEN'),
    apc_code_source_id = ROUND(z.apc_code_source_id, 0, 'ROUND_HALF_EVEN'),
    comparison_calc_level_id = ROUND(z.comparison_calc_level_id, 0, 'ROUND_HALF_EVEN'),
    gov_newtech_profile_id = ROUND(z.gvt_newtech_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_coins_fs_profile_id = ROUND(z.ce_coins_fs_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_snf_profile_id = ROUND(z.ce_snf_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_state_tax_profile_id = ROUND(z.ce_state_tax_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_hierarchy_profile_id = ROUND(z.ce_hierarchy_profile_id, 0, 'ROUND_HALF_EVEN'),
    inpatient_accrual_method_id = ROUND(z.inpatient_accrual_method_id, 0, 'ROUND_HALF_EVEN'),
    outpatient_accrual_method_id = ROUND(z.outpatient_accrual_method_id, 0, 'ROUND_HALF_EVEN'),
    ce_tri_er_profile_id = ROUND(z.ce_tri_er_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_tri_apc_profile_id = ROUND(z.ce_tri_apc_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_atp_profile_id = ROUND(z.ce_atp_profile_id, 0, 'ROUND_HALF_EVEN'),
    ce_tri_drg_code_profile_id = ROUND(z.ce_tri_drg_code_profile_id, 0, 'ROUND_HALF_EVEN'),
    coins_calc_pct_sel_id = ROUND(z.coins_calc_perc_sel_id, 0, 'ROUND_HALF_EVEN'),
    gov_ime_component_id = ROUND(z.gvt_ime_component_id, 0, 'ROUND_HALF_EVEN'),
    cst_share_calc_pct_sel_id = ROUND(z.cst_share_calc_perc_sel_id, 0, 'ROUND_HALF_EVEN'),
    drg_profile_id = ROUND(z.drg_profile_id, 0, 'ROUND_HALF_EVEN'),
    icd_version_desc = substr(z.icd_version_desc, 1, 15),
    cob_capped_ind = substr(z.is_cob_capped_ind, 1, 1),
    model_ind = substr(z.is_model_ind, 1, 1),
    validated_ind = substr(z.is_validated_ind, 1, 1),
    generate_contractual_ind = substr(z.generate_contractual_ind, 1, 1),
    generate_acct_calc_detail_ind = substr(z.generate_acct_calc_detail_ind, 1, 1),
    drg_breakout_transfer_ind = substr(z.drg_breakout_transfer_ind, 1, 1),
    drg_breakout_expired_ind = substr(z.drg_breakout_expired_ind, 1, 1),
    drg_breakout_ama_ind = substr(z.drg_breakout_ama_ind, 1, 1),
    gov_dsh_ime_cst_outlr_ind = substr(z.gvt_dsh_ime_cst_outlr_ind, 1, 1),
    gov_include_scp_ind = substr(z.gvt_include_scp_ind, 1, 1),
    ipf_inc_fsc_in_outlier_ind = substr(z.ipf_inc_fsc_in_outlier_ind, 1, 1),
    ipf_inc_fsa_in_outlier_ind = substr(z.ipf_inc_fsa_in_outlier_ind, 1, 1),
    xfer_drg_capped_std_rate_ind = substr(z.xfer_drg_capped_std_rate_ind, 1, 1),
    apc_exclude_outlier_ind = substr(z.apc_exclude_outlier_ind, 1, 1),
    include_gaf_ind = substr(z.include_gaf_ind, 1, 1),
    irf_rural_facility_ind = substr(z.irf_is_rural_facility_ind, 1, 1),
    irf_teaching_facility_ind = substr(z.irf_is_teaching_facility_ind, 1, 1),
    tri_apc_sole_comm_hosp_ind = substr(z.tri_apc_is_sole_comm_hosp_ind, 1, 1),
    apc_rural_sole_comm_hosp_ind = substr(z.apc_is_rural_sole_comm_hsp_ind, 1, 1),
    gov_transfer_excluded_ind = substr(z.gvt_is_transfer_excluded_ind, 1, 1),
    gov_outlier_excluded_ind = substr(z.gvt_is_outlier_excluded_ind, 1, 1),
    gov_disregard_subterm_ind = substr(z.gvt_is_disregard_subterm_ind, 1, 1),
    valuated_ind = substr(z.is_valuated_ind, 1, 1),
    copay_ind = substr(z.is_copay_ind, 1, 1),
    pat_resp_used_for_cob_ind = substr(z.is_pat_resp_used_for_cob_ind, 1, 1),
    apc_rural_hosp_ind = substr(z.apc_rural_hospital_ind, 1, 1),
    effective_begin_date = z.effective_begin_date,
    effective_end_date = z.effective_end_date,
    validated_date_time = CAST(z.validated_date AS DATETIME),
    cers_term_create_date = z.cers_term_create_date,
    cers_term_update_date = z.cers_term_update_date,
    drg_version = z.drg_version,
    drg_type_code = substr(z.drg_type, 1, 10),
    comparison_calc_modifier_amt = ROUND(z.comparison_calc_modifier_amt, 3, 'ROUND_HALF_EVEN'),
    gov_base_rate_amt = ROUND(z.gvt_base_rate_amt, 3, 'ROUND_HALF_EVEN'),
    gov_labor_rate_amt = ROUND(z.gvt_labor_rate_amt, 3, 'ROUND_HALF_EVEN'),
    gov_wage_index_amt = ROUND(z.gvt_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    gov_nonlabor_index_amt = ROUND(z.gvt_nonlabor_index_amt, 3, 'ROUND_HALF_EVEN'),
    gov_capital_federal_rate_amt = ROUND(z.gvt_capital_federal_rate_amt, 3, 'ROUND_HALF_EVEN'),
    gov_geographical_factor_qty = ROUND(z.gvt_geographical_factor_qty, 3, 'ROUND_HALF_EVEN'),
    gov_urban_add_on_factor_qty = ROUND(z.gvt_urban_add_on_factor_qty, 3, 'ROUND_HALF_EVEN'),
    gov_cst_liv_addon_factor_qty = ROUND(z.gvt_cst_liv_addon_factor_qty, 3, 'ROUND_HALF_EVEN'),
    gov_disp_share_oper_adj_amt = ROUND(z.gvt_disp_share_oper_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_disp_share_capital_adj_amt = ROUND(z.gvt_disp_share_capital_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_ind_med_edu_oper_adj_amt = ROUND(z.gvt_ind_med_edu_oper_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_ind_med_edu_cap_adj_amt = ROUND(z.gvt_ind_med_edu_cap_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_oper_cst_charge_ratio_qty = ROUND(z.gvt_oper_cst_charge_ratio_qty, 3, 'ROUND_HALF_EVEN'),
    gov_cap_cst_charge_ratio_qty = ROUND(z.gvt_cap_cst_charge_ratio_qty, 3, 'ROUND_HALF_EVEN'),
    gov_drg_threshold_adj_amt = ROUND(z.gvt_drg_threshold_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_labor_share_amt = ROUND(z.gvt_labor_share_amt, 3, 'ROUND_HALF_EVEN'),
    gov_disprpr_share_red_adj_amt = ROUND(z.gvt_disprpr_share_red_adj_amt, 3, 'ROUND_HALF_EVEN'),
    gov_oper_cost_of_livng_adj_amt = ROUND(z.gvt_oper_cost_of_livng_adj_amt, 3, 'ROUND_HALF_EVEN'),
    apc_federal_rate_amt = ROUND(z.apc_federal_rate_amt, 3, 'ROUND_HALF_EVEN'),
    apc_wage_index_amt = ROUND(z.apc_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    apc_labor_percent_amt = ROUND(z.apc_labor_percent_amt, 3, 'ROUND_HALF_EVEN'),
    apc_cost_charge_ratio_amt = ROUND(z.apc_cost_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'),
    apc_cost_threshold_amt = ROUND(z.apc_cost_threshold_amt, 3, 'ROUND_HALF_EVEN'),
    apc_cost_outlier_percent_amt = ROUND(z.apc_cost_outlier_percent_amt, 3, 'ROUND_HALF_EVEN'),
    apc_pro_rata_adj_amt = ROUND(z.apc_pro_rata_adj_amt, 3, 'ROUND_HALF_EVEN'),
    apc_coinsurance_limit_amt = ROUND(z.apc_coinsurance_limit_amt, 3, 'ROUND_HALF_EVEN'),
    apc_deductible_amt = ROUND(z.apc_deductible_amt, 3, 'ROUND_HALF_EVEN'),
    gov_scp_rate_amt = ROUND(z.gvt_scp_rate_amt, 3, 'ROUND_HALF_EVEN'),
    apc_fixed_threshold_amt = ROUND(z.apc_fixed_threshold_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_labor_share_amt = ROUND(z.ipf_labor_share_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_nonlabor_share_amt = ROUND(z.ipf_nonlabor_share_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_labor_share_percent_amt = ROUND(z.ipf_labor_share_percent_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_wage_index_amt = ROUND(z.ipf_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_teaching_adj_factor_amt = ROUND(z.ipf_teaching_adj_factor_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_cola_adj_factor_amt = ROUND(z.ipf_cola_adj_factor_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_fixed_loss_amt = ROUND(z.ipf_fixed_loss_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_cost_charge_ratio_amt = ROUND(z.ipf_cost_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_tefra_amt = ROUND(z.ipf_tefra_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_pps_federal_percent_amt = ROUND(z.ipf_pps_federal_percent_amt, 3, 'ROUND_HALF_EVEN'),
    ipf_tefra_hosp_percent_amt = ROUND(z.ipf_tefra_hospital_percent_amt, 3, 'ROUND_HALF_EVEN'),
    gov_noscaasotc_adj_amt = ROUND(z.gvt_noscaasotc_adj_amt, 3, 'ROUND_HALF_EVEN'),
    irf_std_pmt_conv_factor_amt = ROUND(z.irf_std_pmt_conv_factor_amt, 3, 'ROUND_HALF_EVEN'),
    irf_labor_share_amt = ROUND(z.irf_labor_share_amt, 3, 'ROUND_HALF_EVEN'),
    irf_cbsa_wage_index_amt = ROUND(z.irf_cbsa_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    irf_lip_adj_amt = ROUND(z.irf_lip_adj_amt, 3, 'ROUND_HALF_EVEN'),
    irf_cost_to_charge_ratio_amt = ROUND(z.irf_cost_to_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'),
    irf_adj_threshold_amt = ROUND(z.irf_adj_threshold_amt, 3, 'ROUND_HALF_EVEN'),
    irf_teaching_adj_amt = ROUND(z.irf_teaching_adj_amt, 3, 'ROUND_HALF_EVEN'),
    snf_cbsa_wage_index_amt = ROUND(z.snf_cbsa_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    snf_icd9_042_multiplier_amt = ROUND(z.snf_icd9_042_multiplier_amt, 3, 'ROUND_HALF_EVEN'),
    xfer_add_los_qty = ROUND(z.xfer_add_los_qty, 3, 'ROUND_HALF_EVEN'),
    inpatient_accrual_value_amt = ROUND(z.inpatient_accrual_value_amt, 3, 'ROUND_HALF_EVEN'),
    outpatient_accrual_value_amt = ROUND(z.outpatient_accrual_value_amt, 3, 'ROUND_HALF_EVEN'),
    apc_bilateral_disc_fract_amt = ROUND(z.apc_bilateral_disc_fract_amt, 3, 'ROUND_HALF_EVEN'),
    apc_rural_sch_adj_amt = ROUND(z.apc_rural_sch_adj_amt, 3, 'ROUND_HALF_EVEN'),
    apc_terminated_dc_fract_amt = ROUND(z.apc_terminated_disc_fract_amt, 3, 'ROUND_HALF_EVEN'),
    irf_outlier_percent_amt = ROUND(z.irf_outlier_percent_amt, 3, 'ROUND_HALF_EVEN'),
    tri_asa_labor_rel_prtn_amt = ROUND(z.tri_asa_labor_rel_portn_amt, 3, 'ROUND_HALF_EVEN'),
    tri_asa_nonlabor_rel_prtn_amt = ROUND(z.tri_asa_nonlabor_rel_portn_amt, 3, 'ROUND_HALF_EVEN'),
    tri_wage_index_amt = ROUND(z.tri_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    tri_idme_adj_factor_amt = ROUND(z.tri_idme_adj_factor_amt, 3, 'ROUND_HALF_EVEN'),
    tri_shrt_stay_mar_cst_fctr_amt = ROUND(z.tri_shortstay_mar_cst_fctr_amt, 3, 'ROUND_HALF_EVEN'),
    tri_er_wage_index_amt = ROUND(z.tri_er_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    tri_er_labor_portion_amt = ROUND(z.tri_er_labor_portion_amt, 3, 'ROUND_HALF_EVEN'),
    tri_er_nonlabor_portion_amt = ROUND(z.tri_er_nonlabor_portion_amt, 3, 'ROUND_HALF_EVEN'),
    tri_er_cost_share_percent_amt = ROUND(z.tri_er_cost_share_percent_amt, 3, 'ROUND_HALF_EVEN'),
    tri_cost_to_chrg_ratio_amt = ROUND(z.tri_cost_to_chrg_ratio_amt, 3, 'ROUND_HALF_EVEN'),
    tri_noscastic_amt = ROUND(z.tri_noscastic_amt, 3, 'ROUND_HALF_EVEN'),
    tri_labor_share_percent_amt = ROUND(z.tri_labor_share_percent_amt, 3, 'ROUND_HALF_EVEN'),
    tri_nonlabor_share_percent_amt = ROUND(z.tri_nonlabor_share_percent_amt, 3, 'ROUND_HALF_EVEN'),
    tri_apc_wage_index_amt = ROUND(z.tri_apc_wage_index_amt, 3, 'ROUND_HALF_EVEN'),
    tri_apc_cost_chrg_ratio_amt = ROUND(z.tri_apc_cost_chrg_ratio_amt, 3, 'ROUND_HALF_EVEN'),
    tri_apc_sch_adj_amt = ROUND(z.tri_apc_sch_adj_amt, 3, 'ROUND_HALF_EVEN'),
    tri_neonate_factor_amt = ROUND(z.tri_neonate_factor_amt, 3, 'ROUND_HALF_EVEN'),
    gov_subterm_outlier_pct_amt = ROUND(z.gvt_subterm_outlier_pct_amt, 3, 'ROUND_HALF_EVEN'),
    copay_amt = z.copay_amt,
    gov_readm_adj_factor_amt = ROUND(z.gvt_readm_adj_factor_amt, 3, 'ROUND_HALF_EVEN'),
    gov_vbj_adj_factor_amt = z.gvt_vbj_adj_factor_amt,
    dsh_uncmps_care_addon_amt = ROUND(z.dsh_uncmps_care_addon_amt, 3, 'ROUND_HALF_EVEN'),
    implant_ccr_amt = z.implant_ccr_amt,
    low_vol_adj_fctr_amt = z.low_vol_adj_fctr_amt,
    mdh_amt = z.mdh_amt,
    irf_rural_adj_fctr_amt = z.irf_rural_adj_fctr_amt,
    ipf_rural_adj_fctr_amt = z.ipf_rural_adj_fctr_amt,
    calc_base_choice_set_by_usr_nm = z.calc_base_choice_set_by_usr,
    loa_profile_desc = substr(z.loa_profile, 1, 2005),
    comparison_calc_method_user_id = ROUND(z.comparison_calc_method_user_id, 0, 'ROUND_HALF_EVEN'),
    cers_term_activated_date_time = z.cers_term_activated_date_time,
    cers_term_updated_by_user_id = ROUND(z.cers_term_updated_by_user_id, 0, 'ROUND_HALF_EVEN'),
    source_system_code = 'N',
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND) WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        cers_term_id,
        cers_profile_id,
        effective_date_method_id,
        cers_profile_comparison_id,
        comparison_calc_method_id,
        ipf_transition_calc_method_id,
        inpatient_reimb_method_id,
        outpatient_reimb_method_id,
        owner_user_id,
        asc_ranking_method_id,
        validated_by_user_id,
        drg_calc_method_id,
        gov_capital_method_id,
        ce_apc_profile_id,
        status_id,
        ce_profile_fee_schedule_id,
        cers_parent_term_id,
        irf_reimb_method_id,
        ce_irf_profile_id,
        ce_ipf_profile_id,
        drg_transfer_profile_id,
        snf_reimb_method_id,
        cob_method_id,
        apc_code_source_id,
        comparison_calc_level_id,
        gov_newtech_profile_id,
        ce_coins_fs_profile_id,
        ce_snf_profile_id,
        ce_state_tax_profile_id,
        ce_hierarchy_profile_id,
        inpatient_accrual_method_id,
        outpatient_accrual_method_id,
        ce_tri_er_profile_id,
        ce_tri_apc_profile_id,
        ce_atp_profile_id,
        ce_tri_drg_code_profile_id,
        coins_calc_pct_sel_id,
        gov_ime_component_id,
        cst_share_calc_pct_sel_id,
        drg_profile_id,
        icd_version_desc,
        cob_capped_ind,
        model_ind,
        validated_ind,
        generate_contractual_ind,
        generate_acct_calc_detail_ind,
        drg_breakout_transfer_ind,
        drg_breakout_expired_ind,
        drg_breakout_ama_ind,
        gov_dsh_ime_cst_outlr_ind,
        gov_include_scp_ind,
        ipf_inc_fsc_in_outlier_ind,
        ipf_inc_fsa_in_outlier_ind,
        apc_exclude_outlier_ind,
        include_gaf_ind,
        irf_rural_facility_ind,
        irf_teaching_facility_ind,
        tri_apc_sole_comm_hosp_ind,
        apc_rural_sole_comm_hosp_ind,
        gov_transfer_excluded_ind,
        gov_outlier_excluded_ind,
        gov_disregard_subterm_ind,
        valuated_ind,
        copay_ind,
        pat_resp_used_for_cob_ind,
        apc_rural_hosp_ind,
        effective_begin_date,
        effective_end_date,
        validated_date_time,
        cers_term_create_date,
        cers_term_update_date,
        drg_version,
        drg_type_code,
        comparison_calc_modifier_amt,
        gov_base_rate_amt,
        gov_labor_rate_amt,
        gov_wage_index_amt,
        gov_nonlabor_index_amt,
        gov_capital_federal_rate_amt,
        gov_geographical_factor_qty,
        gov_urban_add_on_factor_qty,
        gov_cst_liv_addon_factor_qty,
        gov_disp_share_oper_adj_amt,
        gov_disp_share_capital_adj_amt,
        gov_ind_med_edu_oper_adj_amt,
        gov_ind_med_edu_cap_adj_amt,
        gov_oper_cst_charge_ratio_qty,
        gov_cap_cst_charge_ratio_qty,
        gov_drg_threshold_adj_amt,
        gov_labor_share_amt,
        gov_disprpr_share_red_adj_amt,
        gov_oper_cost_of_livng_adj_amt,
        apc_federal_rate_amt,
        apc_wage_index_amt,
        apc_labor_percent_amt,
        apc_cost_charge_ratio_amt,
        apc_cost_threshold_amt,
        apc_cost_outlier_percent_amt,
        apc_pro_rata_adj_amt,
        apc_coinsurance_limit_amt,
        apc_deductible_amt,
        gov_scp_rate_amt,
        apc_fixed_threshold_amt,
        ipf_labor_share_amt,
        ipf_nonlabor_share_amt,
        ipf_labor_share_percent_amt,
        ipf_wage_index_amt,
        ipf_teaching_adj_factor_amt,
        ipf_cola_adj_factor_amt,
        ipf_fixed_loss_amt,
        ipf_cost_charge_ratio_amt,
        ipf_tefra_amt,
        ipf_pps_federal_percent_amt,
        ipf_tefra_hosp_percent_amt,
        gov_noscaasotc_adj_amt,
        irf_std_pmt_conv_factor_amt,
        irf_labor_share_amt,
        irf_cbsa_wage_index_amt,
        irf_lip_adj_amt,
        irf_cost_to_charge_ratio_amt,
        irf_adj_threshold_amt,
        irf_teaching_adj_amt,
        snf_cbsa_wage_index_amt,
        snf_icd9_042_multiplier_amt,
        xfer_add_los_qty,
        inpatient_accrual_value_amt,
        outpatient_accrual_value_amt,
        apc_bilateral_disc_fract_amt,
        apc_rural_sch_adj_amt,
        apc_terminated_dc_fract_amt,
        irf_outlier_percent_amt,
        tri_asa_labor_rel_prtn_amt,
        tri_asa_nonlabor_rel_prtn_amt,
        tri_wage_index_amt,
        tri_idme_adj_factor_amt,
        tri_shrt_stay_mar_cst_fctr_amt,
        tri_er_wage_index_amt,
        tri_er_labor_portion_amt,
        tri_er_nonlabor_portion_amt,
        tri_er_cost_share_percent_amt,
        tri_cost_to_chrg_ratio_amt,
        tri_noscastic_amt,
        tri_labor_share_percent_amt,
        tri_nonlabor_share_percent_amt,
        tri_apc_wage_index_amt,
        tri_apc_cost_chrg_ratio_amt,
        tri_apc_sch_adj_amt,
        tri_neonate_factor_amt,
        gov_subterm_outlier_pct_amt,
        copay_amt,
        gov_readm_adj_factor_amt,
        gov_vbj_adj_factor_amt,
        dsh_uncmps_care_addon_amt,
        implant_ccr_amt,
        low_vol_adj_fctr_amt,
        mdh_amt,
        irf_rural_adj_fctr_amt,
        ipf_rural_adj_fctr_amt,
        calc_base_choice_set_by_usr_nm,
        xfer_drg_capped_std_rate_ind,
        loa_profile_desc,
        comparison_calc_method_user_id,
        cers_term_activated_date_time,
        cers_term_updated_by_user_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, ROUND(z.ref_cers_term_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ref_cers_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.effective_date_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.cers_profile_comparison_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.comparison_calc_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ipf_transition_calc_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.inpatient_reimb_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.outpatient_reimb_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.owner_user_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.asc_ranking_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.validated_by_user_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.drg_calculation_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.gvt_capital_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_apc_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.status_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_profile_fee_schedule_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.cers_parent_term_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.irf_reimb_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_irf_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_ipf_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.drg_transfer_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.snf_reimb_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.cob_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.apc_code_source_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.comparison_calc_level_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.gvt_newtech_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_coins_fs_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_snf_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_state_tax_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_hierarchy_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.inpatient_accrual_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.outpatient_accrual_method_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_tri_er_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_tri_apc_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_atp_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.ce_tri_drg_code_profile_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.coins_calc_perc_sel_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.gvt_ime_component_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.cst_share_calc_perc_sel_id, 0, 'ROUND_HALF_EVEN'), ROUND(z.drg_profile_id, 0, 'ROUND_HALF_EVEN'), substr(z.icd_version_desc, 1, 15), substr(z.is_cob_capped_ind, 1, 1), substr(z.is_model_ind, 1, 1), substr(z.is_validated_ind, 1, 1), substr(z.generate_contractual_ind, 1, 1), substr(z.generate_acct_calc_detail_ind, 1, 1), substr(z.drg_breakout_transfer_ind, 1, 1), substr(z.drg_breakout_expired_ind, 1, 1), substr(z.drg_breakout_ama_ind, 1, 1), substr(z.gvt_dsh_ime_cst_outlr_ind, 1, 1), substr(z.gvt_include_scp_ind, 1, 1), substr(z.ipf_inc_fsc_in_outlier_ind, 1, 1), substr(z.ipf_inc_fsa_in_outlier_ind, 1, 1), substr(z.apc_exclude_outlier_ind, 1, 1), substr(z.include_gaf_ind, 1, 1), substr(z.irf_is_rural_facility_ind, 1, 1), substr(z.irf_is_teaching_facility_ind, 1, 1), substr(z.tri_apc_is_sole_comm_hosp_ind, 1, 1), substr(z.apc_is_rural_sole_comm_hsp_ind, 1, 1), substr(z.gvt_is_transfer_excluded_ind, 1, 1), substr(z.gvt_is_outlier_excluded_ind, 1, 1), substr(z.gvt_is_disregard_subterm_ind, 1, 1), substr(z.is_valuated_ind, 1, 1), substr(z.is_copay_ind, 1, 1), substr(z.is_pat_resp_used_for_cob_ind, 1, 1), substr(z.apc_rural_hospital_ind, 1, 1), z.effective_begin_date, z.effective_end_date, CAST(z.validated_date AS DATETIME), z.cers_term_create_date, z.cers_term_update_date, z.drg_version, substr(z.drg_type, 1, 10), ROUND(z.comparison_calc_modifier_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_base_rate_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_labor_rate_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_nonlabor_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_capital_federal_rate_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_geographical_factor_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_urban_add_on_factor_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_cst_liv_addon_factor_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_disp_share_oper_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_disp_share_capital_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_ind_med_edu_oper_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_ind_med_edu_cap_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_oper_cst_charge_ratio_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_cap_cst_charge_ratio_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_drg_threshold_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_labor_share_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_disprpr_share_red_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_oper_cost_of_livng_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_federal_rate_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_labor_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_cost_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_cost_threshold_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_cost_outlier_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_pro_rata_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_coinsurance_limit_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_deductible_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_scp_rate_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_fixed_threshold_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_labor_share_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_nonlabor_share_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_labor_share_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_teaching_adj_factor_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_cola_adj_factor_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_fixed_loss_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_cost_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_tefra_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_pps_federal_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.ipf_tefra_hospital_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_noscaasotc_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_std_pmt_conv_factor_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_labor_share_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_cbsa_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_lip_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_cost_to_charge_ratio_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_adj_threshold_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_teaching_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.snf_cbsa_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.snf_icd9_042_multiplier_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.xfer_add_los_qty, 3, 'ROUND_HALF_EVEN'), ROUND(z.inpatient_accrual_value_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.outpatient_accrual_value_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_bilateral_disc_fract_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_rural_sch_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.apc_terminated_disc_fract_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.irf_outlier_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_asa_labor_rel_portn_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_asa_nonlabor_rel_portn_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_idme_adj_factor_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_shortstay_mar_cst_fctr_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_er_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_er_labor_portion_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_er_nonlabor_portion_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_er_cost_share_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_cost_to_chrg_ratio_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_noscastic_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_labor_share_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_nonlabor_share_percent_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_apc_wage_index_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_apc_cost_chrg_ratio_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_apc_sch_adj_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.tri_neonate_factor_amt, 3, 'ROUND_HALF_EVEN'), ROUND(z.gvt_subterm_outlier_pct_amt, 3, 'ROUND_HALF_EVEN'), z.copay_amt, ROUND(z.gvt_readm_adj_factor_amt, 3, 'ROUND_HALF_EVEN'), z.gvt_vbj_adj_factor_amt, ROUND(z.dsh_uncmps_care_addon_amt, 3, 'ROUND_HALF_EVEN'), z.implant_ccr_amt, z.low_vol_adj_fctr_amt, z.mdh_amt, z.irf_rural_adj_fctr_amt, z.ipf_rural_adj_fctr_amt, z.calc_base_choice_set_by_usr, substr(z.xfer_drg_capped_std_rate_ind, 1, 1), substr(z.loa_profile, 1, 2005), ROUND(z.comparison_calc_method_user_id, 0, 'ROUND_HALF_EVEN'), z.cers_term_activated_date_time, ROUND(z.cers_term_updated_by_user_id, 0, 'ROUND_HALF_EVEN'), 'N', datetime_trunc(current_datetime('US/Central'), SECOND));


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT company_code,
             coid,
             cers_term_id
      FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_term
      GROUP BY company_code,
               coid,
               cers_term_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_term');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

--  Status change - Active
BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_term
WHERE ref_cc_cers_term.dw_last_update_date_time <>
    (SELECT max(ref_cc_cers_term_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_cers_term AS ref_cc_cers_term_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;