DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_eor_eapg_calculation.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/*******************************************************************************************************
 Developer: Sean Wilson
      Name: CC_EOR_EAPG_Calculation - BTEQ Script.
      Date: Creation of script on 10/15/2014. SW.
      Mod1: Fixed problems noted in QA for PKG_PER_DIEM_IND and
            TERMNT_PROC_DSCNT_IND on 11/21/2014 SW.
      Mod2: Removed duplicate columns found in QA on 12/8/2014 SW.
      Mod3: Removed CAST on Patient Account number on 1/14/2015 AS.
      Mod4: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on
            4/23/2015 SW.
      Mod5: Procedure Code should be a direct mapping, not Y or N switch.
            Changed on 4/28/2016 SW.
      Mod6: Pymt_Actn_Cd mapping defect resolved on 4/29/2016 SW.
            Added APG_Calc_Output creation date to mapping on 4/29/2016 SW.
            Added No_Outlier_Pymt_Amt to mapping on 4/29/2016 SW.
      Mod7: Mapped columns properly on 05/27/2016 PT.
      Mod8: Fixed TERMNT_PROC_DSCNT_IND to evaluate a number instead of a
            character field to determine Y or N indicator on 6\22\2016 SW.
	  Mod9: Changed delete to only consider active coids on 1/30/2018 SW.
     Mod10: PBI16687 - Revamped EAPG Calculation table for new columns, renamed columns	on 7/9/2018 SW.
	 Mod11: PBI17807 - Fix to allow multiple claim based calculations on 8/6/2018 SW.
	 Mod12: PBI18007 - Request to restrict records to only primary payer as a temporary fix before new
					   PDM is created on 8/6/2018 SW.
	Mod13:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
	Mod14 :Added ' to char hardcoded values on 1/31/2019 PT
	Mod15: Added new columns as part of PBI22247 on 6/20/2019 AM
      Mod16:PBI 25628  - 04/06/2020 - Get Payor ID from Master IPLAN table - EDWRA_BASE_VIEWS.Facility_Iplan (instead of EDWPF_STAGING.PAYOR_ORGANIZATION)
Mod 17 : Audit Merge Change
MOd 18 : Mapping for ICD_Version_Desc and EAPG_Bilateral_Discount_Code has been put 10/06/2020
******************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;
 --Job=CTDRA254;;
 --');
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- diagnostic nodupedgeog on for session;
-- --update edwra_Staging.Apg_Calc_Output set VISIT_ATP_CD=otranslate(VISIT_ATP_CD,otranslate(VISIT_ATP_CD,'0123456789',''),''); -- msk
BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge AS mt USING
  (SELECT DISTINCT cak.patient_dw_id,
                   pyro.payor_dw_id,
                   CAST(mapcl.payer_rank AS INT64) AS insurance_order_num,
                   DATE(mapcl.calculation_date) AS eor_log_date,
                   substr(concat('INS', trim(format('%11d', CAST(mapcl.payer_rank AS INT64)))), 1, 4) AS log_id,
                   CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(trim(format('%11d', CAST(mapcl.payer_rank AS INT64)))) AS INT64) AS log_sequence_num,
                   DATE(mapcl.calculation_date) AS eff_from_date,
                   apgco.apg_calc_output_id AS apg_calc_num,
                   apgco.adj_eapg_wgt AS adjustment_eapg_wgt,
                   apgco.adjmt_fctr AS adjustment_factor_qty,
                   apgco.agcy_edit_cd AS agency_edit_code,
                   ROUND(apgco.ancl_eapg_rt, 3, 'ROUND_HALF_EVEN') AS ancillary_eapg_rate_amt,
                   ROUND(apgco.auto_rt_ehnc_splmt, 3, 'ROUND_HALF_EVEN') AS auto_rate_ehnc_splmt_amt,
                   ROUND(apgco.base_rate, 3, 'ROUND_HALF_EVEN') AS base_rate_amt,
                   ROUND(coalesce(apgco.calc_method_id, CAST(0 AS NUMERIC)), 0, 'ROUND_HALF_EVEN') AS calc_method_id,
                   ROUND(apgco.calc_prfl_id, 0, 'ROUND_HALF_EVEN') AS calc_profile_id,
                   ROUND(coalesce(apgco.ce_rule_id, CAST(0 AS NUMERIC)), 0, 'ROUND_HALF_EVEN') AS ce_rule_id,
                   coalesce(apgco.ce_service_id, CAST(0 AS BIGNUMERIC)) AS ce_service_id,
                   coalesce(apgco.cers_term_id, CAST(0 AS BIGNUMERIC)) AS cers_term_id,
                   apgco.clm_pymt_status_cd AS claim_payment_status_code,
                   substr(CASE
                              WHEN upper(trim(apgco.clm_prcs_cd)) = '0' THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS claim_process_code,
                   rccos.company_code AS company_code,
                   rccos.coid AS coid,
                   apgco.cost_to_chg_ratio AS cost_to_charge_ratio,
                   DATE(apgco.creation_date) AS creation_date,
                   apgco.actn_cd AS eapg_action_code,
                   apgco.biltrl_dscnt_cd AS eapg_bilateral_discount_code, --   msk
 ROUND(apgco.eapg_wgt_cah, 6, 'ROUND_HALF_EVEN') AS eapg_cah_wgt,
 substr(apgco.eapg_category, 1, 2) AS eapg_category_code,
 substr(apgco.eapg_cd, 1, 5) AS eapg_code,
 apgco.code_grpr_vrsn_used,
 apgco.pymt_actn_cd AS payment_actn_code,
 apgco.unassigned_cd,
 substr(apgco.eapg_type, 1, 2) AS eapg_type_code,
 ROUND(apgco.eapg_wgt, 6, 'ROUND_HALF_EVEN') AS eapg_wgt,
 ROUND(apgco.fee_file_id, 0, 'ROUND_HALF_EVEN') AS fee_file_id,
 ROUND(apgco.full_eapg_wgt, 6, 'ROUND_HALF_EVEN') AS full_eapg_wgt,
 substr(ltrim(format('%#7.0f', apgco.grpr_vrsn)), 1, 5) AS grouper_version_code,
 apgco.hosp_type AS hospital_type_text,
 substr(pv.display_text, 1, 15) AS icd_version_desc, --  msk
 CASE
     WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
     ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
 END AS iplan_id,
 substr(CASE
            WHEN apgco.jw_modfr_ind = 1 THEN 'Y'
            ELSE 'N'
        END, 1, 1) AS jw_modifier_ind,
 ROUND(apgco.lab_pymt_amt, 3, 'ROUND_HALF_EVEN') AS lab_payment_amt,
 apgco.li_adj_eapg_wgt,
 ROUND(apgco.li_alt_pymt_amt, 3, 'ROUND_HALF_EVEN') AS li_alternate_payment_amt,
 ROUND(apgco.li_eapg_pymt_amt, 3, 'ROUND_HALF_EVEN') AS li_eapg_payment_amt,
 ROUND(apgco.li_lab_pymt_amt, 3, 'ROUND_HALF_EVEN') AS li_lab_payment_amt,
 ROUND(apgco.li_proc_fee_amt, 3, 'ROUND_HALF_EVEN') AS li_procedure_fee_amt,
 ROUND(apgco.li_rdst_chg_amt, 3, 'ROUND_HALF_EVEN') AS li_redistributed_charge_amt,
 ROUND(apgco.li_user_defn_adjmt_amt, 3, 'ROUND_HALF_EVEN') AS li_user_defined_adj_amt,
 substr(apgco.modfr_cd_1, 1, 2) AS modifier_1_code,
 substr(apgco.modfr_cd_2, 1, 2) AS modifier_2_code,
 substr(apgco.modfr_cd_3, 1, 2) AS modifier_3_code,
 substr(apgco.modfr_cd_4, 1, 2) AS modifier_4_code,
 substr(apgco.modfr_cd_5, 1, 2) AS modifier_5_code,
 ROUND(apgco.no_outlier_pymt_amt, 3, 'ROUND_HALF_EVEN') AS no_outlier_payment_amt,
 ROUND(apgco.noncovd_chg_amt_rank1, 3, 'ROUND_HALF_EVEN') AS noncov_charge_rank_1_amt,
 ROUND(apgco.noncovd_chg_amt_rank2, 3, 'ROUND_HALF_EVEN') AS noncov_charge_rank_2_amt,
 ROUND(apgco.noncovd_chg_amt_rank3, 3, 'ROUND_HALF_EVEN') AS noncov_charge_rank_3_amt,
 CAST(apgco.noncovd_qty_rank1 AS INT64) AS noncov_rank_1_qty,
 CAST(apgco.noncovd_qty_rank2 AS INT64) AS noncov_rank_2_qty,
 CAST(apgco.noncovd_qty_rank3 AS INT64) AS noncov_rank_3_qty,
 apgco.otlr_adj AS outlier_adj_amt,
 ROUND(apgco.apec_otlr_cmpn_amt, 3, 'ROUND_HALF_EVEN') AS pape_outlier_component_amt,
 ROUND(apgco.pape_pymt_amt, 3, 'ROUND_HALF_EVEN') AS pape_payment_amt,
 ROUND(apgco.pape_rt, 3, 'ROUND_HALF_EVEN') AS pape_rate_amt,
 ROUND(CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS NUMERIC), 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
 apgco.pymt_pct,
 substr(CASE
            WHEN apgco.pkg_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS pkg_ind,
 substr(CASE
            WHEN apgco.pkg_per_diem_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS pkg_per_diem_ind,
 apgco.proc_cd AS procedure_cd,
 apgco.prov_plcy_adj AS provider_policy_adj_amt,
 apgco.rbmt_type_cd AS reimbursement_type_code,
 substr(CASE
            WHEN apgco.repeat_anclry_dscnt_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS repeat_anclry_dscnt_ind,
 substr(apgco.rev_cd, 1, 4) AS revenue_code,
 apgco.svc_date AS service_date,
 CAST(apgco.svc_units_pd AS INT64) AS service_units_paid_qty,
 CAST(apgco.svc_units AS INT64) AS service_units_qty,
 substr(CASE
            WHEN apgco.sgnf_clinical_proc_csldtn_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS sgnf_clinical_proc_csldtn_ind,
 substr(CASE
            WHEN apgco.sgnf_proc_csldtn_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS sgnf_proc_csldtn_ind,
 substr(CASE
            WHEN apgco.sgnf_proc_dscnt_cand_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS sgnf_proc_dscnt_cand_ind,
 ROUND(apgco.state_file_id, 0, 'ROUND_HALF_EVEN') AS state_file_id,
 substr(CASE
            WHEN apgco.termnt_proc_dscnt_ind = 0.0 THEN 'N'
            ELSE 'Y'
        END, 1, 1) AS termnt_proc_dc_ind,
 ROUND(apgco.chg_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
 ROUND(apgco.clm_pymt_amt, 3, 'ROUND_HALF_EVEN') AS total_claim_payment_amt,
 ROUND(apgco.covd_chg_amt, 3, 'ROUND_HALF_EVEN') AS total_covered_charge_amt,
 ROUND(apgco.eapg_pymt_amt, 3, 'ROUND_HALF_EVEN') AS total_eapg_payment_amt,
 ROUND(apgco.expt_pymt_amt, 3, 'ROUND_HALF_EVEN') AS total_expected_amt,
 ROUND(apgco.ttl_pymt_amt, 3, 'ROUND_HALF_EVEN') AS total_payment_amt,
 rccos.unit_num AS unit_num,
 ROUND(apgco.user_defn_adjmt_amt, 3, 'ROUND_HALF_EVEN') AS user_defined_adj_amt,
 ROUND(apgco.visit_adj_eapg_wgt, 6, 'ROUND_HALF_EVEN') AS visit_adj_eapg_wgt,
 ROUND(apgco.visit_apec_otlr_cmpn_amt, 3, 'ROUND_HALF_EVEN') AS visit_adj_pape_otlr_cmpnt_amt,
 ROUND(apgco.visit_apec_pymt_amt, 3, 'ROUND_HALF_EVEN') AS visit_adj_pape_payment_amt,
 apgco.visit_atp_cd AS visit_atp_code, --  otranslate(APGCO.VISIT_ATP_CD,otranslate(APGCO.VISIT_ATP_CD,'0123456789',''),'') AS Visit_ATP_Code,
 ROUND(apgco.visit_atp_pymt_amt, 3, 'ROUND_HALF_EVEN') AS visit_atp_payment_amt,
 ROUND(apgco.visit_eapg_pymt_amt, 3, 'ROUND_HALF_EVEN') AS visit_eapg_payment_amt,
 ROUND(apgco.visit_full_eapg_wgt, 6, 'ROUND_HALF_EVEN') AS visit_full_eapg_wgt,
 ROUND(apgco.visit_id, 0, 'ROUND_HALF_EVEN') AS visit_id,
 ROUND(apgco.visit_no_outlier_pymt_amt, 3, 'ROUND_HALF_EVEN') AS visit_no_outlier_payment_amt,
 CAST(apgco.smart_act_red_fac_ind AS INT64) AS smart_act_red_fac_ind,
 CAST(apgco.hcd_dvc_addon_pymt_ind AS INT64) AS hcd_dvc_add_on_pymt_ind,
 ROUND(apgco.smart_act_red_amt, 3, 'ROUND_HALF_EVEN') AS smart_actl_red_amt,
 ROUND(apgco.cost_otlr_pymt_amt, 3, 'ROUND_HALF_EVEN') AS cost_otlr_pymt_amt,
 ROUND(apgco.li_cost_otlr_pymt_amt, 3, 'ROUND_HALF_EVEN') AS li_cost_otlr_pymt_amt,
 ROUND(apgco.li_cost_otlr_thld_amt, 3, 'ROUND_HALF_EVEN') AS li_cost_otlr_thld_amt,
 ROUND(apgco.li_eapg_cost, 3, 'ROUND_HALF_EVEN') AS li_eapg_cost,
 ROUND(apgco.rehab_base_rt, 6, 'ROUND_HALF_EVEN') AS rehab_base_rate,
 ROUND(apgco.psych_base_rt, 6, 'ROUND_HALF_EVEN') AS psych_base_rate,
 apgco.clm_rec_type_cd AS clm_rec_type_code,
 'N' AS source_system_code,
 datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apg_calc_output AS apgco
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcl.id = apgco.mon_acct_payer_calc_summary_id
   AND mapcl.schema_id = apgco.schema_id
   AND mapcl.payer_rank = 1
   AND mapcl.is_survivor = 1
   AND mapcl.is_deleted = 0
   INNER JOIN -- Filter on primary.
 {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv ON mapcl.icd_vrsn_id = pv.id
   AND mapcl.schema_id = pv.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mpyr.id = mapcl.mon_payer_id
   AND mpyr.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.org_id = mapcl.org_id
   AND rccos.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(rccos.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS cak ON upper(rtrim(cak.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(cak.company_code)) = upper(rtrim(rccos.company_code))
   AND cak.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS FLOAT64)) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.payor_dw_id, NUMERIC '0') = coalesce(ms.payor_dw_id, NUMERIC '0')
     AND coalesce(mt.payor_dw_id, NUMERIC '1') = coalesce(ms.payor_dw_id, NUMERIC '1'))
AND (coalesce(mt.insurance_order_num, 0) = coalesce(ms.insurance_order_num, 0)
     AND coalesce(mt.insurance_order_num, 1) = coalesce(ms.insurance_order_num, 1))
AND (coalesce(mt.eor_log_date, DATE '1970-01-01') = coalesce(ms.eor_log_date, DATE '1970-01-01')
     AND coalesce(mt.eor_log_date, DATE '1970-01-02') = coalesce(ms.eor_log_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.log_id, '0')) = upper(coalesce(ms.log_id, '0'))
     AND upper(coalesce(mt.log_id, '1')) = upper(coalesce(ms.log_id, '1')))
AND (coalesce(mt.log_sequence_num, 0) = coalesce(ms.log_sequence_num, 0)
     AND coalesce(mt.log_sequence_num, 1) = coalesce(ms.log_sequence_num, 1))
AND (coalesce(mt.eff_from_date, DATE '1970-01-01') = coalesce(ms.eff_from_date, DATE '1970-01-01')
     AND coalesce(mt.eff_from_date, DATE '1970-01-02') = coalesce(ms.eff_from_date, DATE '1970-01-02'))
AND (coalesce(mt.apg_calc_num, NUMERIC '0') = coalesce(ms.apg_calc_num, NUMERIC '0')
     AND coalesce(mt.apg_calc_num, NUMERIC '1') = coalesce(ms.apg_calc_num, NUMERIC '1'))
AND (coalesce(mt.adjustment_eapg_wgt, NUMERIC '0') = coalesce(ms.adjustment_eapg_wgt, NUMERIC '0')
     AND coalesce(mt.adjustment_eapg_wgt, NUMERIC '1') = coalesce(ms.adjustment_eapg_wgt, NUMERIC '1'))
AND (coalesce(mt.adjustment_factor_qty, NUMERIC '0') = coalesce(ms.adjustment_factor_qty, NUMERIC '0')
     AND coalesce(mt.adjustment_factor_qty, NUMERIC '1') = coalesce(ms.adjustment_factor_qty, NUMERIC '1'))
AND (upper(coalesce(mt.agency_edit_code, '0')) = upper(coalesce(ms.agency_edit_code, '0'))
     AND upper(coalesce(mt.agency_edit_code, '1')) = upper(coalesce(ms.agency_edit_code, '1')))
AND (coalesce(mt.ancillary_eapg_rate_amt, NUMERIC '0') = coalesce(ms.ancillary_eapg_rate_amt, NUMERIC '0')
     AND coalesce(mt.ancillary_eapg_rate_amt, NUMERIC '1') = coalesce(ms.ancillary_eapg_rate_amt, NUMERIC '1'))
AND (coalesce(mt.auto_rate_ehnc_splmt_amt, NUMERIC '0') = coalesce(ms.auto_rate_ehnc_splmt_amt, NUMERIC '0')
     AND coalesce(mt.auto_rate_ehnc_splmt_amt, NUMERIC '1') = coalesce(ms.auto_rate_ehnc_splmt_amt, NUMERIC '1'))
AND (coalesce(mt.base_rate_amt, NUMERIC '0') = coalesce(ms.base_rate_amt, NUMERIC '0')
     AND coalesce(mt.base_rate_amt, NUMERIC '1') = coalesce(ms.base_rate_amt, NUMERIC '1'))
AND (coalesce(mt.calc_method_id, NUMERIC '0') = coalesce(ms.calc_method_id, NUMERIC '0')
     AND coalesce(mt.calc_method_id, NUMERIC '1') = coalesce(ms.calc_method_id, NUMERIC '1'))
AND (coalesce(mt.calc_profile_id, NUMERIC '0') = coalesce(ms.calc_profile_id, NUMERIC '0')
     AND coalesce(mt.calc_profile_id, NUMERIC '1') = coalesce(ms.calc_profile_id, NUMERIC '1'))
AND (coalesce(mt.ce_rule_id, NUMERIC '0') = coalesce(ms.ce_rule_id, NUMERIC '0')
     AND coalesce(mt.ce_rule_id, NUMERIC '1') = coalesce(ms.ce_rule_id, NUMERIC '1'))
AND (coalesce(mt.ce_service_id, NUMERIC '0') = coalesce(ms.ce_service_id, NUMERIC '0')
     AND coalesce(mt.ce_service_id, NUMERIC '1') = coalesce(ms.ce_service_id, NUMERIC '1'))
AND (coalesce(mt.cers_term_id, NUMERIC '0') = coalesce(ms.cers_term_id, NUMERIC '0')
     AND coalesce(mt.cers_term_id, NUMERIC '1') = coalesce(ms.cers_term_id, NUMERIC '1'))
AND (upper(coalesce(mt.claim_payment_status_code, '0')) = upper(coalesce(ms.claim_payment_status_code, '0'))
     AND upper(coalesce(mt.claim_payment_status_code, '1')) = upper(coalesce(ms.claim_payment_status_code, '1')))
AND (upper(coalesce(mt.claim_process_code, '0')) = upper(coalesce(ms.claim_process_code, '0'))
     AND upper(coalesce(mt.claim_process_code, '1')) = upper(coalesce(ms.claim_process_code, '1')))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.cost_to_charge_ratio, NUMERIC '0') = coalesce(ms.cost_to_charge_ratio, NUMERIC '0')
     AND coalesce(mt.cost_to_charge_ratio, NUMERIC '1') = coalesce(ms.cost_to_charge_ratio, NUMERIC '1'))
AND (coalesce(mt.creation_date, DATE '1970-01-01') = coalesce(ms.creation_date, DATE '1970-01-01')
     AND coalesce(mt.creation_date, DATE '1970-01-02') = coalesce(ms.creation_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.eapg_action_code, '0')) = upper(coalesce(ms.eapg_action_code, '0'))
     AND upper(coalesce(mt.eapg_action_code, '1')) = upper(coalesce(ms.eapg_action_code, '1')))
AND (upper(coalesce(mt.eapg_bilateral_discount_code, '0')) = upper(coalesce(ms.eapg_bilateral_discount_code, '0'))
     AND upper(coalesce(mt.eapg_bilateral_discount_code, '1')) = upper(coalesce(ms.eapg_bilateral_discount_code, '1')))
AND (coalesce(mt.eapg_cah_wgt, NUMERIC '0') = coalesce(ms.eapg_cah_wgt, NUMERIC '0')
     AND coalesce(mt.eapg_cah_wgt, NUMERIC '1') = coalesce(ms.eapg_cah_wgt, NUMERIC '1'))
AND (upper(coalesce(mt.eapg_category_code, '0')) = upper(coalesce(ms.eapg_category_code, '0'))
     AND upper(coalesce(mt.eapg_category_code, '1')) = upper(coalesce(ms.eapg_category_code, '1')))
AND (upper(coalesce(mt.eapg_code, '0')) = upper(coalesce(ms.eapg_code, '0'))
     AND upper(coalesce(mt.eapg_code, '1')) = upper(coalesce(ms.eapg_code, '1')))
AND (upper(coalesce(mt.eapg_code_version_text, '0')) = upper(coalesce(ms.code_grpr_vrsn_used, '0'))
     AND upper(coalesce(mt.eapg_code_version_text, '1')) = upper(coalesce(ms.code_grpr_vrsn_used, '1')))
AND (upper(coalesce(mt.eapg_payment_action_code, '0')) = upper(coalesce(ms.payment_actn_code, '0'))
     AND upper(coalesce(mt.eapg_payment_action_code, '1')) = upper(coalesce(ms.payment_actn_code, '1')))
AND (upper(coalesce(mt.eapg_unassigned_code, '0')) = upper(coalesce(ms.unassigned_cd, '0'))
     AND upper(coalesce(mt.eapg_unassigned_code, '1')) = upper(coalesce(ms.unassigned_cd, '1')))
AND (upper(coalesce(mt.eapg_type_code, '0')) = upper(coalesce(ms.eapg_type_code, '0'))
     AND upper(coalesce(mt.eapg_type_code, '1')) = upper(coalesce(ms.eapg_type_code, '1')))
AND (coalesce(mt.eapg_wgt, NUMERIC '0') = coalesce(ms.eapg_wgt, NUMERIC '0')
     AND coalesce(mt.eapg_wgt, NUMERIC '1') = coalesce(ms.eapg_wgt, NUMERIC '1'))
AND (coalesce(mt.fee_file_id, NUMERIC '0') = coalesce(ms.fee_file_id, NUMERIC '0')
     AND coalesce(mt.fee_file_id, NUMERIC '1') = coalesce(ms.fee_file_id, NUMERIC '1'))
AND (coalesce(mt.full_eapg_wgt, NUMERIC '0') = coalesce(ms.full_eapg_wgt, NUMERIC '0')
     AND coalesce(mt.full_eapg_wgt, NUMERIC '1') = coalesce(ms.full_eapg_wgt, NUMERIC '1'))
AND (upper(coalesce(mt.grouper_version_code, '0')) = upper(coalesce(ms.grouper_version_code, '0'))
     AND upper(coalesce(mt.grouper_version_code, '1')) = upper(coalesce(ms.grouper_version_code, '1')))
AND (upper(coalesce(mt.hospital_type_text, '0')) = upper(coalesce(ms.hospital_type_text, '0'))
     AND upper(coalesce(mt.hospital_type_text, '1')) = upper(coalesce(ms.hospital_type_text, '1')))
AND (upper(coalesce(mt.icd_version_desc, '0')) = upper(coalesce(ms.icd_version_desc, '0'))
     AND upper(coalesce(mt.icd_version_desc, '1')) = upper(coalesce(ms.icd_version_desc, '1')))
AND (coalesce(mt.iplan_id, 0) = coalesce(ms.iplan_id, 0)
     AND coalesce(mt.iplan_id, 1) = coalesce(ms.iplan_id, 1))
AND (upper(coalesce(mt.jw_modifier_ind, '0')) = upper(coalesce(ms.jw_modifier_ind, '0'))
     AND upper(coalesce(mt.jw_modifier_ind, '1')) = upper(coalesce(ms.jw_modifier_ind, '1')))
AND (coalesce(mt.lab_payment_amt, NUMERIC '0') = coalesce(ms.lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.lab_payment_amt, NUMERIC '1') = coalesce(ms.lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.li_adj_eapg_wgt, NUMERIC '0') = coalesce(ms.li_adj_eapg_wgt, NUMERIC '0')
     AND coalesce(mt.li_adj_eapg_wgt, NUMERIC '1') = coalesce(ms.li_adj_eapg_wgt, NUMERIC '1'))
AND (coalesce(mt.li_alternate_payment_amt, NUMERIC '0') = coalesce(ms.li_alternate_payment_amt, NUMERIC '0')
     AND coalesce(mt.li_alternate_payment_amt, NUMERIC '1') = coalesce(ms.li_alternate_payment_amt, NUMERIC '1'))
AND (coalesce(mt.li_eapg_payment_amt, NUMERIC '0') = coalesce(ms.li_eapg_payment_amt, NUMERIC '0')
     AND coalesce(mt.li_eapg_payment_amt, NUMERIC '1') = coalesce(ms.li_eapg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.li_lab_payment_amt, NUMERIC '0') = coalesce(ms.li_lab_payment_amt, NUMERIC '0')
     AND coalesce(mt.li_lab_payment_amt, NUMERIC '1') = coalesce(ms.li_lab_payment_amt, NUMERIC '1'))
AND (coalesce(mt.li_procedure_fee_amt, NUMERIC '0') = coalesce(ms.li_procedure_fee_amt, NUMERIC '0')
     AND coalesce(mt.li_procedure_fee_amt, NUMERIC '1') = coalesce(ms.li_procedure_fee_amt, NUMERIC '1'))
AND (coalesce(mt.li_redistributed_charge_amt, NUMERIC '0') = coalesce(ms.li_redistributed_charge_amt, NUMERIC '0')
     AND coalesce(mt.li_redistributed_charge_amt, NUMERIC '1') = coalesce(ms.li_redistributed_charge_amt, NUMERIC '1'))
AND (coalesce(mt.li_user_defined_adj_amt, NUMERIC '0') = coalesce(ms.li_user_defined_adj_amt, NUMERIC '0')
     AND coalesce(mt.li_user_defined_adj_amt, NUMERIC '1') = coalesce(ms.li_user_defined_adj_amt, NUMERIC '1'))
AND (upper(coalesce(mt.modifier_1_code, '0')) = upper(coalesce(ms.modifier_1_code, '0'))
     AND upper(coalesce(mt.modifier_1_code, '1')) = upper(coalesce(ms.modifier_1_code, '1')))
AND (upper(coalesce(mt.modifier_2_code, '0')) = upper(coalesce(ms.modifier_2_code, '0'))
     AND upper(coalesce(mt.modifier_2_code, '1')) = upper(coalesce(ms.modifier_2_code, '1')))
AND (upper(coalesce(mt.modifier_3_code, '0')) = upper(coalesce(ms.modifier_3_code, '0'))
     AND upper(coalesce(mt.modifier_3_code, '1')) = upper(coalesce(ms.modifier_3_code, '1')))
AND (upper(coalesce(mt.modifier_4_code, '0')) = upper(coalesce(ms.modifier_4_code, '0'))
     AND upper(coalesce(mt.modifier_4_code, '1')) = upper(coalesce(ms.modifier_4_code, '1')))
AND (upper(coalesce(mt.modifier_5_code, '0')) = upper(coalesce(ms.modifier_5_code, '0'))
     AND upper(coalesce(mt.modifier_5_code, '1')) = upper(coalesce(ms.modifier_5_code, '1')))
AND (coalesce(mt.no_outlier_payment_amt, NUMERIC '0') = coalesce(ms.no_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.no_outlier_payment_amt, NUMERIC '1') = coalesce(ms.no_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.noncov_charge_rank_1_amt, NUMERIC '0') = coalesce(ms.noncov_charge_rank_1_amt, NUMERIC '0')
     AND coalesce(mt.noncov_charge_rank_1_amt, NUMERIC '1') = coalesce(ms.noncov_charge_rank_1_amt, NUMERIC '1'))
AND (coalesce(mt.noncov_charge_rank_2_amt, NUMERIC '0') = coalesce(ms.noncov_charge_rank_2_amt, NUMERIC '0')
     AND coalesce(mt.noncov_charge_rank_2_amt, NUMERIC '1') = coalesce(ms.noncov_charge_rank_2_amt, NUMERIC '1'))
AND (coalesce(mt.noncov_charge_rank_3_amt, NUMERIC '0') = coalesce(ms.noncov_charge_rank_3_amt, NUMERIC '0')
     AND coalesce(mt.noncov_charge_rank_3_amt, NUMERIC '1') = coalesce(ms.noncov_charge_rank_3_amt, NUMERIC '1'))
AND (coalesce(mt.noncov_rank_1_qty, 0) = coalesce(ms.noncov_rank_1_qty, 0)
     AND coalesce(mt.noncov_rank_1_qty, 1) = coalesce(ms.noncov_rank_1_qty, 1))
AND (coalesce(mt.noncov_rank_2_qty, 0) = coalesce(ms.noncov_rank_2_qty, 0)
     AND coalesce(mt.noncov_rank_2_qty, 1) = coalesce(ms.noncov_rank_2_qty, 1))
AND (coalesce(mt.noncov_rank_3_qty, 0) = coalesce(ms.noncov_rank_3_qty, 0)
     AND coalesce(mt.noncov_rank_3_qty, 1) = coalesce(ms.noncov_rank_3_qty, 1))
AND (coalesce(mt.outlier_adj_amt, NUMERIC '0') = coalesce(ms.outlier_adj_amt, NUMERIC '0')
     AND coalesce(mt.outlier_adj_amt, NUMERIC '1') = coalesce(ms.outlier_adj_amt, NUMERIC '1'))
AND (coalesce(mt.pape_outlier_component_amt, NUMERIC '0') = coalesce(ms.pape_outlier_component_amt, NUMERIC '0')
     AND coalesce(mt.pape_outlier_component_amt, NUMERIC '1') = coalesce(ms.pape_outlier_component_amt, NUMERIC '1'))
AND (coalesce(mt.pape_payment_amt, NUMERIC '0') = coalesce(ms.pape_payment_amt, NUMERIC '0')
     AND coalesce(mt.pape_payment_amt, NUMERIC '1') = coalesce(ms.pape_payment_amt, NUMERIC '1'))
AND (coalesce(mt.pape_rate_amt, NUMERIC '0') = coalesce(ms.pape_rate_amt, NUMERIC '0')
     AND coalesce(mt.pape_rate_amt, NUMERIC '1') = coalesce(ms.pape_rate_amt, NUMERIC '1'))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (coalesce(mt.payment_pct, NUMERIC '0') = coalesce(ms.pymt_pct, NUMERIC '0')
     AND coalesce(mt.payment_pct, NUMERIC '1') = coalesce(ms.pymt_pct, NUMERIC '1'))
AND (upper(coalesce(mt.pkg_ind, '0')) = upper(coalesce(ms.pkg_ind, '0'))
     AND upper(coalesce(mt.pkg_ind, '1')) = upper(coalesce(ms.pkg_ind, '1')))
AND (upper(coalesce(mt.pkg_per_diem_ind, '0')) = upper(coalesce(ms.pkg_per_diem_ind, '0'))
     AND upper(coalesce(mt.pkg_per_diem_ind, '1')) = upper(coalesce(ms.pkg_per_diem_ind, '1')))
AND (upper(coalesce(mt.procedure_code, '0')) = upper(coalesce(ms.procedure_cd, '0'))
     AND upper(coalesce(mt.procedure_code, '1')) = upper(coalesce(ms.procedure_cd, '1')))
AND (coalesce(mt.provider_policy_adj_amt, NUMERIC '0') = coalesce(ms.provider_policy_adj_amt, NUMERIC '0')
     AND coalesce(mt.provider_policy_adj_amt, NUMERIC '1') = coalesce(ms.provider_policy_adj_amt, NUMERIC '1'))
AND (upper(coalesce(mt.reimbursement_type_code, '0')) = upper(coalesce(ms.reimbursement_type_code, '0'))
     AND upper(coalesce(mt.reimbursement_type_code, '1')) = upper(coalesce(ms.reimbursement_type_code, '1')))
AND (upper(coalesce(mt.repeat_anclry_discount_code, '0')) = upper(coalesce(ms.repeat_anclry_dscnt_ind, '0'))
     AND upper(coalesce(mt.repeat_anclry_discount_code, '1')) = upper(coalesce(ms.repeat_anclry_dscnt_ind, '1')))
AND (upper(coalesce(mt.revenue_code, '0')) = upper(coalesce(ms.revenue_code, '0'))
     AND upper(coalesce(mt.revenue_code, '1')) = upper(coalesce(ms.revenue_code, '1')))
AND (coalesce(mt.service_date, DATE '1970-01-01') = coalesce(ms.service_date, DATE '1970-01-01')
     AND coalesce(mt.service_date, DATE '1970-01-02') = coalesce(ms.service_date, DATE '1970-01-02'))
AND (coalesce(mt.service_unit_paid_qty, 0) = coalesce(ms.service_units_paid_qty, 0)
     AND coalesce(mt.service_unit_paid_qty, 1) = coalesce(ms.service_units_paid_qty, 1))
AND (coalesce(mt.service_unit_qty, 0) = coalesce(ms.service_units_qty, 0)
     AND coalesce(mt.service_unit_qty, 1) = coalesce(ms.service_units_qty, 1))
AND (upper(coalesce(mt.sgnf_clnc_proc_csdt_ind, '0')) = upper(coalesce(ms.sgnf_clinical_proc_csldtn_ind, '0'))
     AND upper(coalesce(mt.sgnf_clnc_proc_csdt_ind, '1')) = upper(coalesce(ms.sgnf_clinical_proc_csldtn_ind, '1')))
AND (upper(coalesce(mt.sgnf_proc_csdt_ind, '0')) = upper(coalesce(ms.sgnf_proc_csldtn_ind, '0'))
     AND upper(coalesce(mt.sgnf_proc_csdt_ind, '1')) = upper(coalesce(ms.sgnf_proc_csldtn_ind, '1')))
AND (upper(coalesce(mt.sgnf_proc_dc_cand_ind, '0')) = upper(coalesce(ms.sgnf_proc_dscnt_cand_ind, '0'))
     AND upper(coalesce(mt.sgnf_proc_dc_cand_ind, '1')) = upper(coalesce(ms.sgnf_proc_dscnt_cand_ind, '1')))
AND (coalesce(mt.state_file_id, NUMERIC '0') = coalesce(ms.state_file_id, NUMERIC '0')
     AND coalesce(mt.state_file_id, NUMERIC '1') = coalesce(ms.state_file_id, NUMERIC '1'))
AND (upper(coalesce(mt.termn_proc_dc_ind, '0')) = upper(coalesce(ms.termnt_proc_dc_ind, '0'))
     AND upper(coalesce(mt.termn_proc_dc_ind, '1')) = upper(coalesce(ms.termnt_proc_dc_ind, '1')))
AND (coalesce(mt.total_charge_amt, NUMERIC '0') = coalesce(ms.total_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_charge_amt, NUMERIC '1') = coalesce(ms.total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.total_claim_payment_amt, NUMERIC '0') = coalesce(ms.total_claim_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_claim_payment_amt, NUMERIC '1') = coalesce(ms.total_claim_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_covered_charge_amt, NUMERIC '0') = coalesce(ms.total_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_covered_charge_amt, NUMERIC '1') = coalesce(ms.total_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.total_eapg_payment_amt, NUMERIC '0') = coalesce(ms.total_eapg_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_eapg_payment_amt, NUMERIC '1') = coalesce(ms.total_eapg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.total_expected_payment_amt, NUMERIC '0') = coalesce(ms.total_expected_amt, NUMERIC '0')
     AND coalesce(mt.total_expected_payment_amt, NUMERIC '1') = coalesce(ms.total_expected_amt, NUMERIC '1'))
AND (coalesce(mt.total_payment_amt, NUMERIC '0') = coalesce(ms.total_payment_amt, NUMERIC '0')
     AND coalesce(mt.total_payment_amt, NUMERIC '1') = coalesce(ms.total_payment_amt, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.user_defined_adj_amt, NUMERIC '0') = coalesce(ms.user_defined_adj_amt, NUMERIC '0')
     AND coalesce(mt.user_defined_adj_amt, NUMERIC '1') = coalesce(ms.user_defined_adj_amt, NUMERIC '1'))
AND (coalesce(mt.visit_adj_eapg_wgt, NUMERIC '0') = coalesce(ms.visit_adj_eapg_wgt, NUMERIC '0')
     AND coalesce(mt.visit_adj_eapg_wgt, NUMERIC '1') = coalesce(ms.visit_adj_eapg_wgt, NUMERIC '1'))
AND (coalesce(mt.visit_adj_pape_otlr_cmpnt_amt, NUMERIC '0') = coalesce(ms.visit_adj_pape_otlr_cmpnt_amt, NUMERIC '0')
     AND coalesce(mt.visit_adj_pape_otlr_cmpnt_amt, NUMERIC '1') = coalesce(ms.visit_adj_pape_otlr_cmpnt_amt, NUMERIC '1'))
AND (coalesce(mt.visit_adj_pape_payment_amt, NUMERIC '0') = coalesce(ms.visit_adj_pape_payment_amt, NUMERIC '0')
     AND coalesce(mt.visit_adj_pape_payment_amt, NUMERIC '1') = coalesce(ms.visit_adj_pape_payment_amt, NUMERIC '1'))
AND (upper(coalesce(mt.visit_atp_code, '0')) = upper(coalesce(ms.visit_atp_code, '0'))
     AND upper(coalesce(mt.visit_atp_code, '1')) = upper(coalesce(ms.visit_atp_code, '1')))
AND (coalesce(mt.visit_atp_payment_amt, NUMERIC '0') = coalesce(ms.visit_atp_payment_amt, NUMERIC '0')
     AND coalesce(mt.visit_atp_payment_amt, NUMERIC '1') = coalesce(ms.visit_atp_payment_amt, NUMERIC '1'))
AND (coalesce(mt.visit_eapg_payment_amt, NUMERIC '0') = coalesce(ms.visit_eapg_payment_amt, NUMERIC '0')
     AND coalesce(mt.visit_eapg_payment_amt, NUMERIC '1') = coalesce(ms.visit_eapg_payment_amt, NUMERIC '1'))
AND (coalesce(mt.visit_full_eapg_wgt, NUMERIC '0') = coalesce(ms.visit_full_eapg_wgt, NUMERIC '0')
     AND coalesce(mt.visit_full_eapg_wgt, NUMERIC '1') = coalesce(ms.visit_full_eapg_wgt, NUMERIC '1'))
AND (coalesce(mt.visit_id, NUMERIC '0') = coalesce(ms.visit_id, NUMERIC '0')
     AND coalesce(mt.visit_id, NUMERIC '1') = coalesce(ms.visit_id, NUMERIC '1'))
AND (coalesce(mt.visit_no_outlier_payment_amt, NUMERIC '0') = coalesce(ms.visit_no_outlier_payment_amt, NUMERIC '0')
     AND coalesce(mt.visit_no_outlier_payment_amt, NUMERIC '1') = coalesce(ms.visit_no_outlier_payment_amt, NUMERIC '1'))
AND (coalesce(mt.smart_act_red_fac_ind, 0) = coalesce(ms.smart_act_red_fac_ind, 0)
     AND coalesce(mt.smart_act_red_fac_ind, 1) = coalesce(ms.smart_act_red_fac_ind, 1))
AND (coalesce(mt.hcd_dvc_add_on_pymt_ind, 0) = coalesce(ms.hcd_dvc_add_on_pymt_ind, 0)
     AND coalesce(mt.hcd_dvc_add_on_pymt_ind, 1) = coalesce(ms.hcd_dvc_add_on_pymt_ind, 1))
AND (coalesce(mt.smart_actl_red_amt, NUMERIC '0') = coalesce(ms.smart_actl_red_amt, NUMERIC '0')
     AND coalesce(mt.smart_actl_red_amt, NUMERIC '1') = coalesce(ms.smart_actl_red_amt, NUMERIC '1'))
AND (coalesce(mt.cost_otlr_pymt_amt, NUMERIC '0') = coalesce(ms.cost_otlr_pymt_amt, NUMERIC '0')
     AND coalesce(mt.cost_otlr_pymt_amt, NUMERIC '1') = coalesce(ms.cost_otlr_pymt_amt, NUMERIC '1'))
AND (coalesce(mt.li_cost_otlr_pymt_amt, NUMERIC '0') = coalesce(ms.li_cost_otlr_pymt_amt, NUMERIC '0')
     AND coalesce(mt.li_cost_otlr_pymt_amt, NUMERIC '1') = coalesce(ms.li_cost_otlr_pymt_amt, NUMERIC '1'))
AND (coalesce(mt.li_cost_otlr_thld_amt, NUMERIC '0') = coalesce(ms.li_cost_otlr_thld_amt, NUMERIC '0')
     AND coalesce(mt.li_cost_otlr_thld_amt, NUMERIC '1') = coalesce(ms.li_cost_otlr_thld_amt, NUMERIC '1'))
AND (coalesce(mt.li_eapg_cost, NUMERIC '0') = coalesce(ms.li_eapg_cost, NUMERIC '0')
     AND coalesce(mt.li_eapg_cost, NUMERIC '1') = coalesce(ms.li_eapg_cost, NUMERIC '1'))
AND (coalesce(mt.rehab_base_rate, NUMERIC '0') = coalesce(ms.rehab_base_rate, NUMERIC '0')
     AND coalesce(mt.rehab_base_rate, NUMERIC '1') = coalesce(ms.rehab_base_rate, NUMERIC '1'))
AND (coalesce(mt.psych_base_rate, NUMERIC '0') = coalesce(ms.psych_base_rate, NUMERIC '0')
     AND coalesce(mt.psych_base_rate, NUMERIC '1') = coalesce(ms.psych_base_rate, NUMERIC '1'))
AND (upper(coalesce(mt.clm_rec_type_code, '0')) = upper(coalesce(ms.clm_rec_type_code, '0'))
     AND upper(coalesce(mt.clm_rec_type_code, '1')) = upper(coalesce(ms.clm_rec_type_code, '1')))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01')) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        payor_dw_id,
        insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        apg_calc_num,
        adjustment_eapg_wgt,
        adjustment_factor_qty,
        agency_edit_code,
        ancillary_eapg_rate_amt,
        auto_rate_ehnc_splmt_amt,
        base_rate_amt,
        calc_method_id,
        calc_profile_id,
        ce_rule_id,
        ce_service_id,
        cers_term_id,
        claim_payment_status_code,
        claim_process_code,
        company_code,
        coid,
        cost_to_charge_ratio,
        creation_date,
        eapg_action_code,
        eapg_bilateral_discount_code,
        eapg_cah_wgt,
        eapg_category_code,
        eapg_code,
        eapg_code_version_text,
        eapg_payment_action_code,
        eapg_unassigned_code,
        eapg_type_code,
        eapg_wgt,
        fee_file_id,
        full_eapg_wgt,
        grouper_version_code,
        hospital_type_text,
        icd_version_desc,
        iplan_id,
        jw_modifier_ind,
        lab_payment_amt,
        li_adj_eapg_wgt,
        li_alternate_payment_amt,
        li_eapg_payment_amt,
        li_lab_payment_amt,
        li_procedure_fee_amt,
        li_redistributed_charge_amt,
        li_user_defined_adj_amt,
        modifier_1_code,
        modifier_2_code,
        modifier_3_code,
        modifier_4_code,
        modifier_5_code,
        no_outlier_payment_amt,
        noncov_charge_rank_1_amt,
        noncov_charge_rank_2_amt,
        noncov_charge_rank_3_amt,
        noncov_rank_1_qty,
        noncov_rank_2_qty,
        noncov_rank_3_qty,
        outlier_adj_amt,
        pape_outlier_component_amt,
        pape_payment_amt,
        pape_rate_amt,
        pat_acct_num,
        payment_pct,
        pkg_ind,
        pkg_per_diem_ind,
        procedure_code,
        provider_policy_adj_amt,
        reimbursement_type_code,
        repeat_anclry_discount_code,
        revenue_code,
        service_date,
        service_unit_paid_qty,
        service_unit_qty,
        sgnf_clnc_proc_csdt_ind,
        sgnf_proc_csdt_ind,
        sgnf_proc_dc_cand_ind,
        state_file_id,
        termn_proc_dc_ind,
        total_charge_amt,
        total_claim_payment_amt,
        total_covered_charge_amt,
        total_eapg_payment_amt,
        total_expected_payment_amt,
        total_payment_amt,
        unit_num,
        user_defined_adj_amt,
        visit_adj_eapg_wgt,
        visit_adj_pape_otlr_cmpnt_amt,
        visit_adj_pape_payment_amt,
        visit_atp_code,
        visit_atp_payment_amt,
        visit_eapg_payment_amt,
        visit_full_eapg_wgt,
        visit_id,
        visit_no_outlier_payment_amt,
        smart_act_red_fac_ind,
        hcd_dvc_add_on_pymt_ind,
        smart_actl_red_amt,
        cost_otlr_pymt_amt,
        li_cost_otlr_pymt_amt,
        li_cost_otlr_thld_amt,
        li_eapg_cost,
        rehab_base_rate,
        psych_base_rate,
        clm_rec_type_code,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_dw_id, ms.payor_dw_id, ms.insurance_order_num, ms.eor_log_date, ms.log_id, ms.log_sequence_num, ms.eff_from_date, ms.apg_calc_num, ms.adjustment_eapg_wgt, ms.adjustment_factor_qty, ms.agency_edit_code, ms.ancillary_eapg_rate_amt, ms.auto_rate_ehnc_splmt_amt, ms.base_rate_amt, ms.calc_method_id, ms.calc_profile_id, ms.ce_rule_id, ms.ce_service_id, ms.cers_term_id, ms.claim_payment_status_code, ms.claim_process_code, ms.company_code, ms.coid, ms.cost_to_charge_ratio, ms.creation_date, ms.eapg_action_code, ms.eapg_bilateral_discount_code, ms.eapg_cah_wgt, ms.eapg_category_code, ms.eapg_code, ms.code_grpr_vrsn_used, ms.payment_actn_code, ms.unassigned_cd, ms.eapg_type_code, ms.eapg_wgt, ms.fee_file_id, ms.full_eapg_wgt, ms.grouper_version_code, ms.hospital_type_text, ms.icd_version_desc, ms.iplan_id, ms.jw_modifier_ind, ms.lab_payment_amt, ms.li_adj_eapg_wgt, ms.li_alternate_payment_amt, ms.li_eapg_payment_amt, ms.li_lab_payment_amt, ms.li_procedure_fee_amt, ms.li_redistributed_charge_amt, ms.li_user_defined_adj_amt, ms.modifier_1_code, ms.modifier_2_code, ms.modifier_3_code, ms.modifier_4_code, ms.modifier_5_code, ms.no_outlier_payment_amt, ms.noncov_charge_rank_1_amt, ms.noncov_charge_rank_2_amt, ms.noncov_charge_rank_3_amt, ms.noncov_rank_1_qty, ms.noncov_rank_2_qty, ms.noncov_rank_3_qty, ms.outlier_adj_amt, ms.pape_outlier_component_amt, ms.pape_payment_amt, ms.pape_rate_amt, ms.pat_acct_num, ms.pymt_pct, ms.pkg_ind, ms.pkg_per_diem_ind, ms.procedure_cd, ms.provider_policy_adj_amt, ms.reimbursement_type_code, ms.repeat_anclry_dscnt_ind, ms.revenue_code, ms.service_date, ms.service_units_paid_qty, ms.service_units_qty, ms.sgnf_clinical_proc_csldtn_ind, ms.sgnf_proc_csldtn_ind, ms.sgnf_proc_dscnt_cand_ind, ms.state_file_id, ms.termnt_proc_dc_ind, ms.total_charge_amt, ms.total_claim_payment_amt, ms.total_covered_charge_amt, ms.total_eapg_payment_amt, ms.total_expected_amt, ms.total_payment_amt, ms.unit_num, ms.user_defined_adj_amt, ms.visit_adj_eapg_wgt, ms.visit_adj_pape_otlr_cmpnt_amt, ms.visit_adj_pape_payment_amt, ms.visit_atp_code, ms.visit_atp_payment_amt, ms.visit_eapg_payment_amt, ms.visit_full_eapg_wgt, ms.visit_id, ms.visit_no_outlier_payment_amt, ms.smart_act_red_fac_ind, ms.hcd_dvc_add_on_pymt_ind, ms.smart_actl_red_amt, ms.cost_otlr_pymt_amt, ms.li_cost_otlr_pymt_amt, ms.li_cost_otlr_thld_amt, ms.li_eapg_cost, ms.rehab_base_rate, ms.psych_base_rate, ms.clm_rec_type_code, ms.source_system_code, ms.dw_last_update_date_time) ;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;


BEGIN
SET _ERROR_CODE = 0;

SET srctableid = Null;
SET srctablename = '{{ params.param_parallon_ra_stage_dataset_name }}.apg_calc_output';
SET tgttablename = '{{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge';
SET audit_type= 'RECORD_COUNT';

SET tableload_end_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);
SET tableload_run_time = CAST((tableload_end_time - tableload_start_time) AS STRING);
SET audit_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

SET expected_value = 
(
select count(*) FROM {{ params.param_parallon_ra_stage_dataset_name }}.apg_calc_output AS apgco
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer_calc_latest AS mapcl ON mapcl.id = apgco.mon_acct_payer_calc_summary_id
   AND mapcl.schema_id = apgco.schema_id
   AND mapcl.payer_rank = 1
   AND mapcl.is_survivor = 1
   AND mapcl.is_deleted = 0
   INNER JOIN -- Filter on primary.
{{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pv ON mapcl.icd_vrsn_id = pv.id
   AND mapcl.schema_id = pv.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_payer AS mpyr ON mpyr.id = mapcl.mon_payer_id
   AND mpyr.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rccos ON rccos.org_id = mapcl.org_id
   AND rccos.schema_id = mapcl.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.facility_iplan AS pyro ON upper(rtrim(pyro.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(pyro.company_code)) = upper(rtrim(rccos.company_code))
   AND pyro.iplan_id = CASE
                           WHEN upper(trim(mpyr.code)) = 'NO INS' THEN 0
                           ELSE CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(concat(substr(trim(mpyr.code), 1, 3), substr(trim(mpyr.code), 5, 2))) AS INT64)
                       END
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS cak ON upper(rtrim(cak.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(cak.company_code)) = upper(rtrim(rccos.company_code))
   AND cak.pat_acct_num = CAST(`{{ params.param_bqutil_fns_dataset_name }}`.cw_td_normalize_number(mapcl.account_no) AS FLOAT64)-- cast as FLOAT64
);

SET actual_value =
(
select count(*) as row_count
from {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge
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

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;


IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

--  AUDIT MERGE 'S MERGE
BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_calculation AS x USING
  (SELECT cc_eor_eapg_calculation_merge.patient_dw_id,
          cc_eor_eapg_calculation_merge.payor_dw_id,
          cc_eor_eapg_calculation_merge.insurance_order_num,
          cc_eor_eapg_calculation_merge.eor_log_date,
          cc_eor_eapg_calculation_merge.log_id,
          cc_eor_eapg_calculation_merge.log_sequence_num,
          cc_eor_eapg_calculation_merge.eff_from_date,
          cc_eor_eapg_calculation_merge.apg_calc_num,
          cc_eor_eapg_calculation_merge.adjustment_eapg_wgt,
          cc_eor_eapg_calculation_merge.adjustment_factor_qty,
          cc_eor_eapg_calculation_merge.agency_edit_code,
          cc_eor_eapg_calculation_merge.ancillary_eapg_rate_amt,
          cc_eor_eapg_calculation_merge.auto_rate_ehnc_splmt_amt,
          cc_eor_eapg_calculation_merge.base_rate_amt,
          cc_eor_eapg_calculation_merge.calc_method_id,
          cc_eor_eapg_calculation_merge.calc_profile_id,
          cc_eor_eapg_calculation_merge.ce_rule_id,
          cc_eor_eapg_calculation_merge.ce_service_id,
          cc_eor_eapg_calculation_merge.cers_term_id,
          cc_eor_eapg_calculation_merge.claim_payment_status_code,
          cc_eor_eapg_calculation_merge.claim_process_code,
          cc_eor_eapg_calculation_merge.company_code,
          cc_eor_eapg_calculation_merge.coid,
          cc_eor_eapg_calculation_merge.cost_to_charge_ratio,
          cc_eor_eapg_calculation_merge.creation_date,
          cc_eor_eapg_calculation_merge.eapg_action_code,
          cc_eor_eapg_calculation_merge.eapg_bilateral_discount_code,
          cc_eor_eapg_calculation_merge.eapg_cah_wgt,
          cc_eor_eapg_calculation_merge.eapg_category_code,
          cc_eor_eapg_calculation_merge.eapg_code,
          cc_eor_eapg_calculation_merge.eapg_code_version_text,
          cc_eor_eapg_calculation_merge.eapg_payment_action_code,
          cc_eor_eapg_calculation_merge.eapg_unassigned_code,
          cc_eor_eapg_calculation_merge.eapg_type_code,
          cc_eor_eapg_calculation_merge.eapg_wgt,
          cc_eor_eapg_calculation_merge.fee_file_id,
          cc_eor_eapg_calculation_merge.full_eapg_wgt,
          cc_eor_eapg_calculation_merge.grouper_version_code,
          cc_eor_eapg_calculation_merge.hospital_type_text,
          cc_eor_eapg_calculation_merge.icd_version_desc,
          cc_eor_eapg_calculation_merge.iplan_id,
          cc_eor_eapg_calculation_merge.jw_modifier_ind,
          cc_eor_eapg_calculation_merge.lab_payment_amt,
          cc_eor_eapg_calculation_merge.li_adj_eapg_wgt,
          cc_eor_eapg_calculation_merge.li_alternate_payment_amt,
          cc_eor_eapg_calculation_merge.li_eapg_payment_amt,
          cc_eor_eapg_calculation_merge.li_lab_payment_amt,
          cc_eor_eapg_calculation_merge.li_procedure_fee_amt,
          cc_eor_eapg_calculation_merge.li_redistributed_charge_amt,
          cc_eor_eapg_calculation_merge.li_user_defined_adj_amt,
          cc_eor_eapg_calculation_merge.modifier_1_code,
          cc_eor_eapg_calculation_merge.modifier_2_code,
          cc_eor_eapg_calculation_merge.modifier_3_code,
          cc_eor_eapg_calculation_merge.modifier_4_code,
          cc_eor_eapg_calculation_merge.modifier_5_code,
          cc_eor_eapg_calculation_merge.no_outlier_payment_amt,
          cc_eor_eapg_calculation_merge.noncov_charge_rank_1_amt,
          cc_eor_eapg_calculation_merge.noncov_charge_rank_2_amt,
          cc_eor_eapg_calculation_merge.noncov_charge_rank_3_amt,
          cc_eor_eapg_calculation_merge.noncov_rank_1_qty,
          cc_eor_eapg_calculation_merge.noncov_rank_2_qty,
          cc_eor_eapg_calculation_merge.noncov_rank_3_qty,
          cc_eor_eapg_calculation_merge.outlier_adj_amt,
          cc_eor_eapg_calculation_merge.pape_outlier_component_amt,
          cc_eor_eapg_calculation_merge.pape_payment_amt,
          cc_eor_eapg_calculation_merge.pape_rate_amt,
          cc_eor_eapg_calculation_merge.pat_acct_num,
          cc_eor_eapg_calculation_merge.payment_pct,
          cc_eor_eapg_calculation_merge.pkg_ind,
          cc_eor_eapg_calculation_merge.pkg_per_diem_ind,
          cc_eor_eapg_calculation_merge.procedure_code,
          cc_eor_eapg_calculation_merge.provider_policy_adj_amt,
          cc_eor_eapg_calculation_merge.reimbursement_type_code,
          cc_eor_eapg_calculation_merge.repeat_anclry_discount_code,
          cc_eor_eapg_calculation_merge.revenue_code,
          cc_eor_eapg_calculation_merge.service_date,
          cc_eor_eapg_calculation_merge.service_unit_paid_qty,
          cc_eor_eapg_calculation_merge.service_unit_qty,
          cc_eor_eapg_calculation_merge.sgnf_clnc_proc_csdt_ind,
          cc_eor_eapg_calculation_merge.sgnf_proc_csdt_ind,
          cc_eor_eapg_calculation_merge.sgnf_proc_dc_cand_ind,
          cc_eor_eapg_calculation_merge.state_file_id,
          cc_eor_eapg_calculation_merge.termn_proc_dc_ind,
          cc_eor_eapg_calculation_merge.total_charge_amt,
          cc_eor_eapg_calculation_merge.total_claim_payment_amt,
          cc_eor_eapg_calculation_merge.total_covered_charge_amt,
          cc_eor_eapg_calculation_merge.total_eapg_payment_amt,
          cc_eor_eapg_calculation_merge.total_expected_payment_amt,
          cc_eor_eapg_calculation_merge.total_payment_amt,
          cc_eor_eapg_calculation_merge.unit_num,
          cc_eor_eapg_calculation_merge.user_defined_adj_amt,
          cc_eor_eapg_calculation_merge.visit_adj_eapg_wgt,
          cc_eor_eapg_calculation_merge.visit_adj_pape_otlr_cmpnt_amt,
          cc_eor_eapg_calculation_merge.visit_adj_pape_payment_amt,
          cc_eor_eapg_calculation_merge.visit_atp_code,
          cc_eor_eapg_calculation_merge.visit_atp_payment_amt,
          cc_eor_eapg_calculation_merge.visit_eapg_payment_amt,
          cc_eor_eapg_calculation_merge.visit_full_eapg_wgt,
          cc_eor_eapg_calculation_merge.visit_id,
          cc_eor_eapg_calculation_merge.visit_no_outlier_payment_amt,
          cc_eor_eapg_calculation_merge.smart_act_red_fac_ind,
          cc_eor_eapg_calculation_merge.hcd_dvc_add_on_pymt_ind,
          cc_eor_eapg_calculation_merge.smart_actl_red_amt,
          cc_eor_eapg_calculation_merge.cost_otlr_pymt_amt,
          cc_eor_eapg_calculation_merge.li_cost_otlr_pymt_amt,
          cc_eor_eapg_calculation_merge.li_cost_otlr_thld_amt,
          cc_eor_eapg_calculation_merge.li_eapg_cost,
          cc_eor_eapg_calculation_merge.rehab_base_rate,
          cc_eor_eapg_calculation_merge.psych_base_rate,
          cc_eor_eapg_calculation_merge.clm_rec_type_code,
          cc_eor_eapg_calculation_merge.source_system_code,
          cc_eor_eapg_calculation_merge.dw_last_update_date_time
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge) AS z ON upper(rtrim(x.company_code)) = upper(rtrim(z.company_code))
AND upper(rtrim(x.coid)) = upper(rtrim(z.coid))
AND x.patient_dw_id = z.patient_dw_id
AND x.payor_dw_id = z.payor_dw_id
AND x.insurance_order_num = z.insurance_order_num
AND x.eor_log_date = z.eor_log_date
AND upper(rtrim(x.log_id)) = upper(rtrim(z.log_id))
AND x.log_sequence_num = z.log_sequence_num
AND x.eff_from_date = z.eff_from_date
AND x.apg_calc_num = z.apg_calc_num WHEN MATCHED THEN
UPDATE
SET unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    iplan_id = z.iplan_id,
    pkg_ind = z.pkg_ind,
    sgnf_clnc_proc_csdt_ind = z.sgnf_clnc_proc_csdt_ind,
    sgnf_proc_csdt_ind = z.sgnf_proc_csdt_ind,
    sgnf_proc_dc_cand_ind = z.sgnf_proc_dc_cand_ind,
    termn_proc_dc_ind = z.termn_proc_dc_ind,
    pkg_per_diem_ind = z.pkg_per_diem_ind,
    eapg_category_code = z.eapg_category_code,
    claim_process_code = z.claim_process_code,
    procedure_code = z.procedure_code,
    repeat_anclry_discount_code = z.repeat_anclry_discount_code,
    eapg_payment_action_code = z.eapg_payment_action_code,
    eapg_code = z.eapg_code,
    revenue_code = z.revenue_code,
    eapg_unassigned_code = z.eapg_unassigned_code,
    eapg_type_code = z.eapg_type_code,
    modifier_1_code = z.modifier_1_code,
    modifier_2_code = z.modifier_2_code,
    modifier_3_code = z.modifier_3_code,
    modifier_4_code = z.modifier_4_code,
    modifier_5_code = z.modifier_5_code,
    eapg_code_version_text = z.eapg_code_version_text,
    adjustment_eapg_wgt = z.adjustment_eapg_wgt,
    eapg_wgt = z.eapg_wgt,
    full_eapg_wgt = z.full_eapg_wgt,
    visit_adj_eapg_wgt = z.visit_adj_eapg_wgt,
    visit_full_eapg_wgt = z.visit_full_eapg_wgt,
    payment_pct = z.payment_pct,
    base_rate_amt = z.base_rate_amt,
    total_charge_amt = z.total_charge_amt,
    total_claim_payment_amt = z.total_claim_payment_amt,
    total_covered_charge_amt = z.total_covered_charge_amt,
    total_eapg_payment_amt = z.total_eapg_payment_amt,
    total_expected_payment_amt = z.total_expected_payment_amt,
    visit_eapg_payment_amt = z.visit_eapg_payment_amt,
    visit_no_outlier_payment_amt = z.visit_no_outlier_payment_amt,
    noncov_charge_rank_1_amt = z.noncov_charge_rank_1_amt,
    noncov_charge_rank_2_amt = z.noncov_charge_rank_2_amt,
    noncov_charge_rank_3_amt = z.noncov_charge_rank_3_amt,
    service_unit_qty = z.service_unit_qty,
    service_unit_paid_qty = z.service_unit_paid_qty,
    noncov_rank_1_qty = z.noncov_rank_1_qty,
    noncov_rank_2_qty = z.noncov_rank_2_qty,
    noncov_rank_3_qty = z.noncov_rank_3_qty,
    service_date = z.service_date,
    calc_method_id = z.calc_method_id,
    calc_profile_id = z.calc_profile_id,
    ce_rule_id = z.ce_rule_id,
    ce_service_id = z.ce_service_id,
    cers_term_id = z.cers_term_id,
    state_file_id = z.state_file_id,
    visit_id = z.visit_id,
    fee_file_id = z.fee_file_id,
    creation_date = z.creation_date,
    no_outlier_payment_amt = z.no_outlier_payment_amt,
    li_adj_eapg_wgt = z.li_adj_eapg_wgt,
    li_eapg_payment_amt = z.li_eapg_payment_amt,
    adjustment_factor_qty = z.adjustment_factor_qty,
    agency_edit_code = z.agency_edit_code,
    ancillary_eapg_rate_amt = z.ancillary_eapg_rate_amt,
    pape_outlier_component_amt = z.pape_outlier_component_amt,
    auto_rate_ehnc_splmt_amt = z.auto_rate_ehnc_splmt_amt,
    claim_payment_status_code = z.claim_payment_status_code,
    cost_to_charge_ratio = z.cost_to_charge_ratio,
    eapg_cah_wgt = z.eapg_cah_wgt,
    grouper_version_code = z.grouper_version_code,
    hospital_type_text = z.hospital_type_text,
    icd_version_desc = z.icd_version_desc,
    jw_modifier_ind = z.jw_modifier_ind,
    lab_payment_amt = z.lab_payment_amt,
    li_alternate_payment_amt = z.li_alternate_payment_amt,
    li_lab_payment_amt = z.li_lab_payment_amt,
    li_procedure_fee_amt = z.li_procedure_fee_amt,
    li_redistributed_charge_amt = z.li_redistributed_charge_amt,
    li_user_defined_adj_amt = z.li_user_defined_adj_amt,
    outlier_adj_amt = z.outlier_adj_amt,
    pape_payment_amt = z.pape_payment_amt,
    pape_rate_amt = z.pape_rate_amt,
    provider_policy_adj_amt = z.provider_policy_adj_amt,
    reimbursement_type_code = z.reimbursement_type_code,
    total_payment_amt = z.total_payment_amt,
    user_defined_adj_amt = z.user_defined_adj_amt,
    visit_adj_pape_otlr_cmpnt_amt = z.visit_adj_pape_otlr_cmpnt_amt,
    visit_adj_pape_payment_amt = z.visit_adj_pape_payment_amt,
    visit_atp_code = z.visit_atp_code,
    visit_atp_payment_amt = z.visit_atp_payment_amt,
    eapg_action_code = z.eapg_action_code,
    eapg_bilateral_discount_code = z.eapg_bilateral_discount_code,
    smart_act_red_fac_ind = z.smart_act_red_fac_ind,
    hcd_dvc_add_on_pymt_ind = z.hcd_dvc_add_on_pymt_ind,
    smart_actl_red_amt = z.smart_actl_red_amt,
    cost_otlr_pymt_amt = z.cost_otlr_pymt_amt,
    li_cost_otlr_pymt_amt = z.li_cost_otlr_pymt_amt,
    li_cost_otlr_thld_amt = z.li_cost_otlr_thld_amt,
    li_eapg_cost = z.li_eapg_cost,
    clm_rec_type_code = z.clm_rec_type_code,
    psych_base_rate = z.psych_base_rate,
    rehab_base_rate = z.rehab_base_rate,
    source_system_code = z.source_system_code,
    dw_last_update_date_time = z.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (company_code,
        coid,
        patient_dw_id,
        payor_dw_id,
        insurance_order_num,
        eor_log_date,
        log_id,
        log_sequence_num,
        eff_from_date,
        apg_calc_num,
        unit_num,
        pat_acct_num,
        iplan_id,
        pkg_ind,
        sgnf_clnc_proc_csdt_ind,
        sgnf_proc_csdt_ind,
        sgnf_proc_dc_cand_ind,
        termn_proc_dc_ind,
        pkg_per_diem_ind,
        eapg_category_code,
        claim_process_code,
        procedure_code,
        repeat_anclry_discount_code,
        eapg_payment_action_code,
        eapg_code,
        revenue_code,
        eapg_unassigned_code,
        eapg_type_code,
        modifier_1_code,
        modifier_2_code,
        modifier_3_code,
        modifier_4_code,
        modifier_5_code,
        eapg_code_version_text,
        adjustment_eapg_wgt,
        eapg_wgt,
        full_eapg_wgt,
        visit_adj_eapg_wgt,
        visit_full_eapg_wgt,
        payment_pct,
        base_rate_amt,
        total_charge_amt,
        total_claim_payment_amt,
        total_covered_charge_amt,
        total_eapg_payment_amt,
        total_expected_payment_amt,
        visit_eapg_payment_amt,
        visit_no_outlier_payment_amt,
        noncov_charge_rank_1_amt,
        noncov_charge_rank_2_amt,
        noncov_charge_rank_3_amt,
        service_unit_qty,
        service_unit_paid_qty,
        noncov_rank_1_qty,
        noncov_rank_2_qty,
        noncov_rank_3_qty,
        service_date,
        calc_method_id,
        calc_profile_id,
        ce_rule_id,
        ce_service_id,
        cers_term_id,
        state_file_id,
        visit_id,
        fee_file_id,
        creation_date,
        no_outlier_payment_amt,
        li_adj_eapg_wgt,
        li_eapg_payment_amt,
        adjustment_factor_qty,
        agency_edit_code,
        ancillary_eapg_rate_amt,
        pape_outlier_component_amt,
        auto_rate_ehnc_splmt_amt,
        claim_payment_status_code,
        cost_to_charge_ratio,
        eapg_cah_wgt,
        grouper_version_code,
        hospital_type_text,
        icd_version_desc,
        jw_modifier_ind,
        lab_payment_amt,
        li_alternate_payment_amt,
        li_lab_payment_amt,
        li_procedure_fee_amt,
        li_redistributed_charge_amt,
        li_user_defined_adj_amt,
        outlier_adj_amt,
        pape_payment_amt,
        pape_rate_amt,
        provider_policy_adj_amt,
        reimbursement_type_code,
        total_payment_amt,
        user_defined_adj_amt,
        visit_adj_pape_otlr_cmpnt_amt,
        visit_adj_pape_payment_amt,
        visit_atp_code,
        visit_atp_payment_amt,
        eapg_action_code,
        eapg_bilateral_discount_code,
        smart_act_red_fac_ind,
        hcd_dvc_add_on_pymt_ind,
        smart_actl_red_amt,
        cost_otlr_pymt_amt,
        li_cost_otlr_pymt_amt,
        li_cost_otlr_thld_amt,
        li_eapg_cost,
        clm_rec_type_code,
        psych_base_rate,
        rehab_base_rate,
        source_system_code,
        dw_last_update_date_time)
VALUES (z.company_code, z.coid, z.patient_dw_id, z.payor_dw_id, z.insurance_order_num, z.eor_log_date, z.log_id, z.log_sequence_num, z.eff_from_date, z.apg_calc_num, z.unit_num, z.pat_acct_num, z.iplan_id, z.pkg_ind, z.sgnf_clnc_proc_csdt_ind, z.sgnf_proc_csdt_ind, z.sgnf_proc_dc_cand_ind, z.termn_proc_dc_ind, z.pkg_per_diem_ind, z.eapg_category_code, z.claim_process_code, z.procedure_code, z.repeat_anclry_discount_code, z.eapg_payment_action_code, z.eapg_code, z.revenue_code, z.eapg_unassigned_code, z.eapg_type_code, z.modifier_1_code, z.modifier_2_code, z.modifier_3_code, z.modifier_4_code, z.modifier_5_code, z.eapg_code_version_text, z.adjustment_eapg_wgt, z.eapg_wgt, z.full_eapg_wgt, z.visit_adj_eapg_wgt, z.visit_full_eapg_wgt, z.payment_pct, z.base_rate_amt, z.total_charge_amt, z.total_claim_payment_amt, z.total_covered_charge_amt, z.total_eapg_payment_amt, z.total_expected_payment_amt, z.visit_eapg_payment_amt, z.visit_no_outlier_payment_amt, z.noncov_charge_rank_1_amt, z.noncov_charge_rank_2_amt, z.noncov_charge_rank_3_amt, z.service_unit_qty, z.service_unit_paid_qty, z.noncov_rank_1_qty, z.noncov_rank_2_qty, z.noncov_rank_3_qty, z.service_date, z.calc_method_id, z.calc_profile_id, z.ce_rule_id, z.ce_service_id, z.cers_term_id, z.state_file_id, z.visit_id, z.fee_file_id, z.creation_date, z.no_outlier_payment_amt, z.li_adj_eapg_wgt, z.li_eapg_payment_amt, z.adjustment_factor_qty, z.agency_edit_code, z.ancillary_eapg_rate_amt, z.pape_outlier_component_amt, z.auto_rate_ehnc_splmt_amt, z.claim_payment_status_code, z.cost_to_charge_ratio, z.eapg_cah_wgt, z.grouper_version_code, z.hospital_type_text, z.icd_version_desc, z.jw_modifier_ind, z.lab_payment_amt, z.li_alternate_payment_amt, z.li_lab_payment_amt, z.li_procedure_fee_amt, z.li_redistributed_charge_amt, z.li_user_defined_adj_amt, z.outlier_adj_amt, z.pape_payment_amt, z.pape_rate_amt, z.provider_policy_adj_amt, z.reimbursement_type_code, z.total_payment_amt, z.user_defined_adj_amt, z.visit_adj_pape_otlr_cmpnt_amt, z.visit_adj_pape_payment_amt, z.visit_atp_code, z.visit_atp_payment_amt, z.eapg_action_code, z.eapg_bilateral_discount_code, z.smart_act_red_fac_ind, z.hcd_dvc_add_on_pymt_ind, z.smart_actl_red_amt, z.cost_otlr_pymt_amt, z.li_cost_otlr_pymt_amt, z.li_cost_otlr_thld_amt, z.li_eapg_cost, z.clm_rec_type_code, z.psych_base_rate, z.rehab_base_rate, z.source_system_code, z.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             payor_dw_id,
             insurance_order_num,
             eor_log_date,
             log_id,
             log_sequence_num,
             eff_from_date,
             apg_calc_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_calculation
      GROUP BY patient_dw_id,
               payor_dw_id,
               insurance_order_num,
               eor_log_date,
               log_id,
               log_sequence_num,
               eff_from_date,
               apg_calc_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_calculation');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_calculation
WHERE upper(cc_eor_eapg_calculation.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_eor_eapg_calculation.dw_last_update_date_time <>
    (SELECT max(cc_eor_eapg_calculation_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_calculation AS cc_eor_eapg_calculation_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_EOR_EAPG_Calculation');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_calculation_merge;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;