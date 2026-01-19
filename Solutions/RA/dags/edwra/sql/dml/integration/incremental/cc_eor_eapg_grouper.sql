DECLARE DUP_COUNT INT64;

DECLARE srctableid, tolerance_percent, idx, idx_length INT64 DEFAULT 0;
DECLARE expected_value, actual_value, difference NUMERIC;
DECLARE sourcesysnm, srcdataset_id, srctablename, tgtdataset_id, tgttablename, audit_type, tableload_run_time, job_name, audit_status STRING;
DECLARE tableload_start_time, tableload_end_time, audit_time, current_ts DATETIME;

-- Translation time: 2024-02-23T20:57:40.989496Z
-- Translation job ID: 33350c4d-0680-433e-a7c7-05a342c11922
-- Source: eim-parallon-cs-datamig-dev-0002/ra_bteqs_bulk_conversion/RoknIl/input/cc_eor_eapg_grouper.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

/****************************************************************************************************************************************************
 Developer: Sean Wilson
      Name: CC_EOR_EAPG_Grouper - BTEQ Script.
      Date: Creation of script on 10/15/2014. SW.
   Purpose: Builds the CC_EOR_EAPG_Grouper table used within the Business Objects AD-HOC Universe
            for reporting.
      Mod1: Added delete statement for purge process on 11/20/2014 SW.
      Mod2: Added code to ignore insurance plans with a rank of 50 or above
            on 11/24/2014 SW.
      Mod3: Added left outer join to Ref_CC_EAPG_Unassigned_Code to get EAPG_Unassigned_Code
            and EAPG_Unassigned_Code_Desc on 11/25/2014 SW.
      Mod4: Removed Unassigned_Code_Desc from update/insert.  We are using a reference table
            to get this information on 12/16/2014 SW.
      Mod5: Optimization fix to add schema_id to ref_cc_org_structure and joins to clinical_acctkeys
            on 2/26/2015 SW.
      Mod6: Changed to use EDWRA_STAGING.Clinical_Acctkeys for performance on 4/23/2015 SW.
      Mod7: CR152 - ICD10 - Adding new column ICD_Version_Desc -  09/30/2015  jac
      Mod8: Added COID join for Ref_CC_EAPG_Unassigned_Code table -  03/15/2016  jac
      Mod9: Added Code_Type on 4/26/2016 SW.
      Mod10:Removed unnecessary join with Ref_CC_EAPG_Unassigned_Code and changed column names with respect to CC_EOR_EAPG_Grouper on 05/26/2016 PT.
      Mod11:Changed column Repeat_Ancl_Dc_Ind to an integer from a char(1)	-07/13/2016 PT
      Mod12:Added column Secn_Diag_Used_Code_Desc to the table	-07/13/2016 PT
      Mod13:PDM change.  Added apg_grpr_output creation_date on 6/9/2017 SW.
      Mod14:Fix for warning in the delete step for checking error code on 6/29/2017 SW.
	  Mod15:Updated code for modified PDM by removing Payor_DW_ID, Iplan_Id,and Iplan_Insurance_Order_Num. Removed filter for primary insurance on
	        09/07/2017 SW.
      Mod16:Defect:4168 - Added missing source column ACTN_CD for EAPG_Action_Code to script. This column was not being populated as caught by QA on
	        10/6/2017 SW.
      Mod17:Changed delete to only consider active coids on 1/31/2018 SW.
      Mod18:PBI16687 - Revamped EAPG Grouper table for new columns, renamed columns	on 7/9/2018 SW.
      Mod19:PBI18007 - Request to restrict records to only primary payer as a temporary fix before new
					   PDM is created on 8/6/2018 SW.
	Mod20:Changed Query Band Statement to have Audit job name for increase in priority on teradata side and ease of understanding for DBA's on 9/22/2018 PT.
Mod 21: Added ' to char hardcoded values on 1/31/2019 PT
Mod 22: Added Audit Merge on 07/15/2020 PT
*****************************************************************************************************************************************************/ -- CALL dbadmin_procs.SET_QUERY_BAND('App=RA_Group2_ETL;Job=CTDRA261;');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

SET tableload_start_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_grouper_stg AS mt USING
  (SELECT DISTINCT cak.patient_dw_id,
                   apgo.apg_grpr_output_id AS apg_grouper_num,
                   rccos.company_code AS company_code,
                   apgo.coid AS coid,
                   apgo.pat_acct_num,
                   rccos.unit_num AS unit_num,
                   apgo.svc_date AS service_date,
                   substr(apgo.eapg_cd, 1, 5) AS eapg_code,
                   substr(apgo.eapg_type, 1, 2) AS eapg_type_code,
                   substr(apgo.eapg_category, 1, 2) AS eapg_category_code,
                   substr(CASE
                              WHEN apgo.drug_304b_dscnt_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS drug_304b_dscnt_ind,
                   substr(CASE
                              WHEN apgo.pkg_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS pkg_ind,
                   substr(CASE
                              WHEN apgo.pkg_per_diem_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS pkg_per_diem_ind,
                   CAST(apgo.repeat_anclry_dscnt_ind AS INT64) AS repeat_ancl_dc_ind,
                   substr(CASE
                              WHEN apgo.sgnf_clinical_proc_csldtn_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS sgnf_clinical_proc_csldtn_ind,
                   substr(CASE
                              WHEN apgo.sgnf_proc_csldtn_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS sgnf_proc_csldtn_ind,
                   substr(CASE
                              WHEN apgo.sgnf_proc_dscnt_cand_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS sgnf_proc_discnt_cand_ind,
                   substr(CASE
                              WHEN apgo.termnt_proc_dscnt_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS termnt_proc_dscnt_ind,
                   substr(CASE
                              WHEN apgo.visit_prvtv_med_diag_pres_ind = 0.0 THEN 'N'
                              ELSE 'Y'
                          END, 1, 1) AS visit_prvtv_med_diag_pres_ind,
                   substr(CASE
                              WHEN apgo.mon_acct_chg_dtl_id IS NOT NULL
                                   OR apgo.mon_acct_code_id IS NOT NULL THEN 'Charge'
                              WHEN apgo.claim_charge_ub_id IS NOT NULL
                                   OR apgo.claim_code_id IS NOT NULL THEN 'Claim'
                          END, 1, 10) AS charge_claim_ind,
                   substr(CASE
                              WHEN apgo.mon_acct_chg_dtl_id IS NOT NULL
                                   OR apgo.claim_charge_ub_id IS NOT NULL THEN 'Charge'
                              WHEN apgo.mon_acct_code_id IS NOT NULL
                                   OR apgo.claim_code_id IS NOT NULL THEN 'Code'
                          END, 1, 10) AS charge_ind,
                   apgo.code_grpr_vrsn_used,
                   apgo.biltrl_dscnt_cd,
                   substr(apgo.prin_diag_cd_used, 1, 7) AS principal_diagnosis_code,
                   substr(apgo.med_visit_diag_cd, 1, 10) AS med_visit_diagnosis_code,
                   apgo.secn_diag_cd_used AS secn_diagnosis_code,
                   substr(apgo.rev_cd, 1, 4) AS revenue_code,
                   apgo.unassigned_cd AS unassigned_code,
                   substr(apgo.used_proc_cd, 1, 10) AS used_procedure_code,
                   apgo.visit_diag_cd_used AS used_visit_diagnosis_code,
                   apgo.visit_type_cd AS visit_type_code,
                   substr(apgo.charge_code, 1, 10) AS charge_code_desc,
                   substr(apgo.modfr_cd_1, 1, 2) AS modifier_code_1,
                   substr(apgo.modfr_cd_2, 1, 2) AS modifier_code_2,
                   substr(apgo.modfr_cd_3, 1, 2) AS modifier_code_3,
                   substr(apgo.modfr_cd_4, 1, 2) AS modifier_code_4,
                   substr(apgo.modfr_cd_5, 1, 2) AS modifier_code_5,
                   ROUND(apgo.chg_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
                   ROUND(apgo.covd_chg_amt, 3, 'ROUND_HALF_EVEN') AS total_covered_charge_amt,
                   ROUND(apgo.noncovd_chg_amt_rank1, 3, 'ROUND_HALF_EVEN') AS noncovered_charge_amt_rank1,
                   ROUND(apgo.noncovd_chg_amt_rank2, 3, 'ROUND_HALF_EVEN') AS noncovered_charge_amt_rank2,
                   ROUND(apgo.noncovd_chg_amt_rank3, 3, 'ROUND_HALF_EVEN') AS noncovered_charge_amt_rank3,
                   CAST(apgo.noncovd_qty_rank1 AS INT64) AS noncovered_charge_qty_rank1,
                   CAST(apgo.noncovd_qty_rank2 AS INT64) AS noncovered_charge_qty_rank2,
                   CAST(apgo.noncovd_qty_rank3 AS INT64) AS noncovered_charge_qty_rank3,
                   CAST(apgo.num_of_visits AS INT64) AS visit_cnt,
                   CAST(apgo.proc_cd_used_cnt AS INT64) AS procedure_code_used_qty,
                   CAST(apgo.secn_diag_cd_used_cnt AS INT64) AS secn_diagnosis_code_used_qty,
                   CAST(apgo.svc_units AS INT64) AS service_unit_qty,
                   CAST(apgo.visit_lines AS INT64) AS visit_line_qty,
                   apgo.charge_posting_date AS charge_posted_date,
                   ROUND(apgo.visit_id, 0, 'ROUND_HALF_EVEN') AS visit_id,
                   ROUND(apgo.calc_meth_id, 0, 'ROUND_HALF_EVEN') AS calc_method_id,
                   ROUND(apgo.calc_prfl_id, 0, 'ROUND_HALF_EVEN') AS calc_profile_id,
                   substr(pvicd.display_text, 1, 15) AS icd_version_desc,
                   apgo.code_type,
                   apgo.creation_date AS creation_date_time,
                   apgo.actn_cd AS eapg_action_code,
                   substr(CASE
                              WHEN apgo.jw_modfr_ind = 1 THEN 'Y'
                              ELSE 'N'
                          END, 1, 1) AS jw_modfr_ind,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time,
                   'N' AS source_system_code
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.apg_grpr_output AS apgo
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.mon_account_payer AS mapy ON apgo.mon_account_id = mapy.mon_account_id
   AND apgo.pyr_id = mapy.mon_payer_id
   AND apgo.schema_id = mapy.schema_id
   LEFT OUTER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.preset_value AS pvicd ON pvicd.id = apgo.icd_vrsn_id
   AND pvicd.schema_id = apgo.schema_id
   INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.ref_cc_org_structure AS rccos ON upper(rtrim(rccos.coid)) = upper(rtrim(apgo.coid))
   AND rccos.schema_id = apgo.schema_id
   INNER JOIN {{ params.param_parallon_ra_stage_dataset_name }}.clinical_acctkeys AS cak ON upper(rtrim(cak.coid)) = upper(rtrim(rccos.coid))
   AND upper(rtrim(cak.company_code)) = upper(rtrim(rccos.company_code))
   AND cak.pat_acct_num = apgo.pat_acct_num
   WHERE mapy.payer_rank = 1
     OR mapy.payer_rank IS NULL ) AS ms ON coalesce(mt.patient_dw_id, NUMERIC '0') = coalesce(ms.patient_dw_id, NUMERIC '0')
AND coalesce(mt.patient_dw_id, NUMERIC '1') = coalesce(ms.patient_dw_id, NUMERIC '1')
AND (coalesce(mt.apg_grouper_num, NUMERIC '0') = coalesce(ms.apg_grouper_num, NUMERIC '0')
     AND coalesce(mt.apg_grouper_num, NUMERIC '1') = coalesce(ms.apg_grouper_num, NUMERIC '1'))
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND (upper(coalesce(mt.coid, '0')) = upper(coalesce(ms.coid, '0'))
     AND upper(coalesce(mt.coid, '1')) = upper(coalesce(ms.coid, '1')))
AND (coalesce(mt.pat_acct_num, NUMERIC '0') = coalesce(ms.pat_acct_num, NUMERIC '0')
     AND coalesce(mt.pat_acct_num, NUMERIC '1') = coalesce(ms.pat_acct_num, NUMERIC '1'))
AND (upper(coalesce(mt.unit_num, '0')) = upper(coalesce(ms.unit_num, '0'))
     AND upper(coalesce(mt.unit_num, '1')) = upper(coalesce(ms.unit_num, '1')))
AND (coalesce(mt.service_date, DATE '1970-01-01') = coalesce(ms.service_date, DATE '1970-01-01')
     AND coalesce(mt.service_date, DATE '1970-01-02') = coalesce(ms.service_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.eapg_code, '0')) = upper(coalesce(ms.eapg_code, '0'))
     AND upper(coalesce(mt.eapg_code, '1')) = upper(coalesce(ms.eapg_code, '1')))
AND (upper(coalesce(mt.eapg_type_code, '0')) = upper(coalesce(ms.eapg_type_code, '0'))
     AND upper(coalesce(mt.eapg_type_code, '1')) = upper(coalesce(ms.eapg_type_code, '1')))
AND (upper(coalesce(mt.eapg_category_code, '0')) = upper(coalesce(ms.eapg_category_code, '0'))
     AND upper(coalesce(mt.eapg_category_code, '1')) = upper(coalesce(ms.eapg_category_code, '1')))
AND (upper(coalesce(mt.drug_304b_dc_ind, '0')) = upper(coalesce(ms.drug_304b_dscnt_ind, '0'))
     AND upper(coalesce(mt.drug_304b_dc_ind, '1')) = upper(coalesce(ms.drug_304b_dscnt_ind, '1')))
AND (upper(coalesce(mt.pkg_ind, '0')) = upper(coalesce(ms.pkg_ind, '0'))
     AND upper(coalesce(mt.pkg_ind, '1')) = upper(coalesce(ms.pkg_ind, '1')))
AND (upper(coalesce(mt.pkg_per_diem_ind, '0')) = upper(coalesce(ms.pkg_per_diem_ind, '0'))
     AND upper(coalesce(mt.pkg_per_diem_ind, '1')) = upper(coalesce(ms.pkg_per_diem_ind, '1')))
AND (coalesce(mt.repeat_ancl_dc_ind, 0) = coalesce(ms.repeat_ancl_dc_ind, 0)
     AND coalesce(mt.repeat_ancl_dc_ind, 1) = coalesce(ms.repeat_ancl_dc_ind, 1))
AND (upper(coalesce(mt.sgnf_clinc_proc_csdt_ind, '0')) = upper(coalesce(ms.sgnf_clinical_proc_csldtn_ind, '0'))
     AND upper(coalesce(mt.sgnf_clinc_proc_csdt_ind, '1')) = upper(coalesce(ms.sgnf_clinical_proc_csldtn_ind, '1')))
AND (upper(coalesce(mt.sgnf_proc_csdt_ind, '0')) = upper(coalesce(ms.sgnf_proc_csldtn_ind, '0'))
     AND upper(coalesce(mt.sgnf_proc_csdt_ind, '1')) = upper(coalesce(ms.sgnf_proc_csldtn_ind, '1')))
AND (upper(coalesce(mt.sgnf_proc_dc_cand_ind, '0')) = upper(coalesce(ms.sgnf_proc_discnt_cand_ind, '0'))
     AND upper(coalesce(mt.sgnf_proc_dc_cand_ind, '1')) = upper(coalesce(ms.sgnf_proc_discnt_cand_ind, '1')))
AND (upper(coalesce(mt.termn_proc_dc_ind, '0')) = upper(coalesce(ms.termnt_proc_dscnt_ind, '0'))
     AND upper(coalesce(mt.termn_proc_dc_ind, '1')) = upper(coalesce(ms.termnt_proc_dscnt_ind, '1')))
AND (upper(coalesce(mt.visit_preventive_med_diag_ind, '0')) = upper(coalesce(ms.visit_prvtv_med_diag_pres_ind, '0'))
     AND upper(coalesce(mt.visit_preventive_med_diag_ind, '1')) = upper(coalesce(ms.visit_prvtv_med_diag_pres_ind, '1')))
AND (upper(coalesce(mt.charge_claim_ind, '0')) = upper(coalesce(ms.charge_claim_ind, '0'))
     AND upper(coalesce(mt.charge_claim_ind, '1')) = upper(coalesce(ms.charge_claim_ind, '1')))
AND (upper(coalesce(mt.charge_ind_code, '0')) = upper(coalesce(ms.charge_ind, '0'))
     AND upper(coalesce(mt.charge_ind_code, '1')) = upper(coalesce(ms.charge_ind, '1')))
AND (upper(coalesce(mt.eapg_code_version_text, '0')) = upper(coalesce(ms.code_grpr_vrsn_used, '0'))
     AND upper(coalesce(mt.eapg_code_version_text, '1')) = upper(coalesce(ms.code_grpr_vrsn_used, '1')))
AND (upper(coalesce(mt.eapg_bilateral_discount_code, '0')) = upper(coalesce(ms.biltrl_dscnt_cd, '0'))
     AND upper(coalesce(mt.eapg_bilateral_discount_code, '1')) = upper(coalesce(ms.biltrl_dscnt_cd, '1')))
AND (upper(coalesce(mt.principal_diag_code, '0')) = upper(coalesce(ms.principal_diagnosis_code, '0'))
     AND upper(coalesce(mt.principal_diag_code, '1')) = upper(coalesce(ms.principal_diagnosis_code, '1')))
AND (upper(coalesce(mt.med_visit_diag_code, '0')) = upper(coalesce(ms.med_visit_diagnosis_code, '0'))
     AND upper(coalesce(mt.med_visit_diag_code, '1')) = upper(coalesce(ms.med_visit_diagnosis_code, '1')))
AND (upper(coalesce(mt.secn_diag_used_code_desc, '0')) = upper(coalesce(ms.secn_diagnosis_code, '0'))
     AND upper(coalesce(mt.secn_diag_used_code_desc, '1')) = upper(coalesce(ms.secn_diagnosis_code, '1')))
AND (upper(coalesce(mt.revenue_code, '0')) = upper(coalesce(ms.revenue_code, '0'))
     AND upper(coalesce(mt.revenue_code, '1')) = upper(coalesce(ms.revenue_code, '1')))
AND (upper(coalesce(mt.eapg_unassigned_code, '0')) = upper(coalesce(ms.unassigned_code, '0'))
     AND upper(coalesce(mt.eapg_unassigned_code, '1')) = upper(coalesce(ms.unassigned_code, '1')))
AND (upper(coalesce(mt.used_proc_code, '0')) = upper(coalesce(ms.used_procedure_code, '0'))
     AND upper(coalesce(mt.used_proc_code, '1')) = upper(coalesce(ms.used_procedure_code, '1')))
AND (upper(coalesce(mt.used_visit_diag_code_desc, '0')) = upper(coalesce(ms.used_visit_diagnosis_code, '0'))
     AND upper(coalesce(mt.used_visit_diag_code_desc, '1')) = upper(coalesce(ms.used_visit_diagnosis_code, '1')))
AND (upper(coalesce(mt.eapg_visit_type_code, '0')) = upper(coalesce(ms.visit_type_code, '0'))
     AND upper(coalesce(mt.eapg_visit_type_code, '1')) = upper(coalesce(ms.visit_type_code, '1')))
AND (upper(coalesce(mt.charge_code_desc, '0')) = upper(coalesce(ms.charge_code_desc, '0'))
     AND upper(coalesce(mt.charge_code_desc, '1')) = upper(coalesce(ms.charge_code_desc, '1')))
AND (upper(coalesce(mt.modifier_1_code, '0')) = upper(coalesce(ms.modifier_code_1, '0'))
     AND upper(coalesce(mt.modifier_1_code, '1')) = upper(coalesce(ms.modifier_code_1, '1')))
AND (upper(coalesce(mt.modifier_2_code, '0')) = upper(coalesce(ms.modifier_code_2, '0'))
     AND upper(coalesce(mt.modifier_2_code, '1')) = upper(coalesce(ms.modifier_code_2, '1')))
AND (upper(coalesce(mt.modifier_3_code, '0')) = upper(coalesce(ms.modifier_code_3, '0'))
     AND upper(coalesce(mt.modifier_3_code, '1')) = upper(coalesce(ms.modifier_code_3, '1')))
AND (upper(coalesce(mt.modifier_4_code, '0')) = upper(coalesce(ms.modifier_code_4, '0'))
     AND upper(coalesce(mt.modifier_4_code, '1')) = upper(coalesce(ms.modifier_code_4, '1')))
AND (upper(coalesce(mt.modifier_5_code, '0')) = upper(coalesce(ms.modifier_code_5, '0'))
     AND upper(coalesce(mt.modifier_5_code, '1')) = upper(coalesce(ms.modifier_code_5, '1')))
AND (coalesce(mt.total_charge_amt, NUMERIC '0') = coalesce(ms.total_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_charge_amt, NUMERIC '1') = coalesce(ms.total_charge_amt, NUMERIC '1'))
AND (coalesce(mt.total_covered_charge_amt, NUMERIC '0') = coalesce(ms.total_covered_charge_amt, NUMERIC '0')
     AND coalesce(mt.total_covered_charge_amt, NUMERIC '1') = coalesce(ms.total_covered_charge_amt, NUMERIC '1'))
AND (coalesce(mt.non_cov_charge_rank_1_amt, NUMERIC '0') = coalesce(ms.noncovered_charge_amt_rank1, NUMERIC '0')
     AND coalesce(mt.non_cov_charge_rank_1_amt, NUMERIC '1') = coalesce(ms.noncovered_charge_amt_rank1, NUMERIC '1'))
AND (coalesce(mt.non_cov_charge_rank_2_amt, NUMERIC '0') = coalesce(ms.noncovered_charge_amt_rank2, NUMERIC '0')
     AND coalesce(mt.non_cov_charge_rank_2_amt, NUMERIC '1') = coalesce(ms.noncovered_charge_amt_rank2, NUMERIC '1'))
AND (coalesce(mt.non_cov_charge_rank_3_amt, NUMERIC '0') = coalesce(ms.noncovered_charge_amt_rank3, NUMERIC '0')
     AND coalesce(mt.non_cov_charge_rank_3_amt, NUMERIC '1') = coalesce(ms.noncovered_charge_amt_rank3, NUMERIC '1'))
AND (coalesce(mt.non_cov_rank_1_qty, 0) = coalesce(ms.noncovered_charge_qty_rank1, 0)
     AND coalesce(mt.non_cov_rank_1_qty, 1) = coalesce(ms.noncovered_charge_qty_rank1, 1))
AND (coalesce(mt.non_cov_rank_2_qty, 0) = coalesce(ms.noncovered_charge_qty_rank2, 0)
     AND coalesce(mt.non_cov_rank_2_qty, 1) = coalesce(ms.noncovered_charge_qty_rank2, 1))
AND (coalesce(mt.non_cov_rank_3_qty, 0) = coalesce(ms.noncovered_charge_qty_rank3, 0)
     AND coalesce(mt.non_cov_rank_3_qty, 1) = coalesce(ms.noncovered_charge_qty_rank3, 1))
AND (coalesce(mt.visit_cnt, 0) = coalesce(ms.visit_cnt, 0)
     AND coalesce(mt.visit_cnt, 1) = coalesce(ms.visit_cnt, 1))
AND (coalesce(mt.proc_code_used_qty, 0) = coalesce(ms.procedure_code_used_qty, 0)
     AND coalesce(mt.proc_code_used_qty, 1) = coalesce(ms.procedure_code_used_qty, 1))
AND (coalesce(mt.secn_diag_code_used_qty, 0) = coalesce(ms.secn_diagnosis_code_used_qty, 0)
     AND coalesce(mt.secn_diag_code_used_qty, 1) = coalesce(ms.secn_diagnosis_code_used_qty, 1))
AND (coalesce(mt.service_unit_qty, 0) = coalesce(ms.service_unit_qty, 0)
     AND coalesce(mt.service_unit_qty, 1) = coalesce(ms.service_unit_qty, 1))
AND (coalesce(mt.visit_line_qty, 0) = coalesce(ms.visit_line_qty, 0)
     AND coalesce(mt.visit_line_qty, 1) = coalesce(ms.visit_line_qty, 1))
AND (coalesce(mt.charge_posted_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.charge_posted_date, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.charge_posted_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.charge_posted_date, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.visit_id, NUMERIC '0') = coalesce(ms.visit_id, NUMERIC '0')
     AND coalesce(mt.visit_id, NUMERIC '1') = coalesce(ms.visit_id, NUMERIC '1'))
AND (coalesce(mt.calc_method_id, NUMERIC '0') = coalesce(ms.calc_method_id, NUMERIC '0')
     AND coalesce(mt.calc_method_id, NUMERIC '1') = coalesce(ms.calc_method_id, NUMERIC '1'))
AND (coalesce(mt.calc_profile_id, NUMERIC '0') = coalesce(ms.calc_profile_id, NUMERIC '0')
     AND coalesce(mt.calc_profile_id, NUMERIC '1') = coalesce(ms.calc_profile_id, NUMERIC '1'))
AND (upper(coalesce(mt.icd_version_desc, '0')) = upper(coalesce(ms.icd_version_desc, '0'))
     AND upper(coalesce(mt.icd_version_desc, '1')) = upper(coalesce(ms.icd_version_desc, '1')))
AND (upper(coalesce(mt.code_type, '0')) = upper(coalesce(ms.code_type, '0'))
     AND upper(coalesce(mt.code_type, '1')) = upper(coalesce(ms.code_type, '1')))
AND (coalesce(mt.creation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.creation_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.creation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.creation_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.eapg_action_code, '0')) = upper(coalesce(ms.eapg_action_code, '0'))
     AND upper(coalesce(mt.eapg_action_code, '1')) = upper(coalesce(ms.eapg_action_code, '1')))
AND (upper(coalesce(mt.jw_modifier_ind, '0')) = upper(coalesce(ms.jw_modfr_ind, '0'))
     AND upper(coalesce(mt.jw_modifier_ind, '1')) = upper(coalesce(ms.jw_modfr_ind, '1')))
AND (coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.dw_last_update_date_time, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.source_system_code, '0')) = upper(coalesce(ms.source_system_code, '0'))
     AND upper(coalesce(mt.source_system_code, '1')) = upper(coalesce(ms.source_system_code, '1'))) WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        apg_grouper_num,
        company_code,
        coid,
        pat_acct_num,
        unit_num,
        service_date,
        eapg_code,
        eapg_type_code,
        eapg_category_code,
        drug_304b_dc_ind,
        pkg_ind,
        pkg_per_diem_ind,
        repeat_ancl_dc_ind,
        sgnf_clinc_proc_csdt_ind,
        sgnf_proc_csdt_ind,
        sgnf_proc_dc_cand_ind,
        termn_proc_dc_ind,
        visit_preventive_med_diag_ind,
        charge_claim_ind,
        charge_ind_code,
        eapg_code_version_text,
        eapg_bilateral_discount_code,
        principal_diag_code,
        med_visit_diag_code,
        secn_diag_used_code_desc,
        revenue_code,
        eapg_unassigned_code,
        used_proc_code,
        used_visit_diag_code_desc,
        eapg_visit_type_code,
        charge_code_desc,
        modifier_1_code,
        modifier_2_code,
        modifier_3_code,
        modifier_4_code,
        modifier_5_code,
        total_charge_amt,
        total_covered_charge_amt,
        non_cov_charge_rank_1_amt,
        non_cov_charge_rank_2_amt,
        non_cov_charge_rank_3_amt,
        non_cov_rank_1_qty,
        non_cov_rank_2_qty,
        non_cov_rank_3_qty,
        visit_cnt,
        proc_code_used_qty,
        secn_diag_code_used_qty,
        service_unit_qty,
        visit_line_qty,
        charge_posted_date_time,
        visit_id,
        calc_method_id,
        calc_profile_id,
        icd_version_desc,
        code_type,
        creation_date_time,
        eapg_action_code,
        jw_modifier_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (ms.patient_dw_id, ms.apg_grouper_num, ms.company_code, ms.coid, ms.pat_acct_num, ms.unit_num, ms.service_date, ms.eapg_code, ms.eapg_type_code, ms.eapg_category_code, ms.drug_304b_dscnt_ind, ms.pkg_ind, ms.pkg_per_diem_ind, ms.repeat_ancl_dc_ind, ms.sgnf_clinical_proc_csldtn_ind, ms.sgnf_proc_csldtn_ind, ms.sgnf_proc_discnt_cand_ind, ms.termnt_proc_dscnt_ind, ms.visit_prvtv_med_diag_pres_ind, ms.charge_claim_ind, ms.charge_ind, ms.code_grpr_vrsn_used, ms.biltrl_dscnt_cd, ms.principal_diagnosis_code, ms.med_visit_diagnosis_code, ms.secn_diagnosis_code, ms.revenue_code, ms.unassigned_code, ms.used_procedure_code, ms.used_visit_diagnosis_code, ms.visit_type_code, ms.charge_code_desc, ms.modifier_code_1, ms.modifier_code_2, ms.modifier_code_3, ms.modifier_code_4, ms.modifier_code_5, ms.total_charge_amt, ms.total_covered_charge_amt, ms.noncovered_charge_amt_rank1, ms.noncovered_charge_amt_rank2, ms.noncovered_charge_amt_rank3, ms.noncovered_charge_qty_rank1, ms.noncovered_charge_qty_rank2, ms.noncovered_charge_qty_rank3, ms.visit_cnt, ms.procedure_code_used_qty, ms.secn_diagnosis_code_used_qty, ms.service_unit_qty, ms.visit_line_qty, ms.charge_posted_date, ms.visit_id, ms.calc_method_id, ms.calc_profile_id, ms.icd_version_desc, ms.code_type, ms.creation_date_time, ms.eapg_action_code, ms.jw_modfr_ind, ms.dw_last_update_date_time, ms.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             apg_grouper_num
      FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_grouper_stg
      GROUP BY patient_dw_id,
               apg_grouper_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_grouper_stg');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('EDWRA_STAGING','CC_EOR_EAPG_Grouper_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_grouper AS x USING
  (SELECT cc_eor_eapg_grouper_stg.*
   FROM {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_grouper_stg) AS z ON x.patient_dw_id = z.patient_dw_id
AND x.apg_grouper_num = z.apg_grouper_num WHEN MATCHED THEN
UPDATE
SET company_code = z.company_code,
    coid = z.coid,
    unit_num = z.unit_num,
    pat_acct_num = z.pat_acct_num,
    service_date = z.service_date,
    eapg_code = z.eapg_code,
    eapg_type_code = z.eapg_type_code,
    eapg_category_code = z.eapg_category_code,
    eapg_code_version_text = z.eapg_code_version_text,
    revenue_code = z.revenue_code,
    eapg_unassigned_code = z.eapg_unassigned_code,
    used_proc_code = z.used_proc_code,
    principal_diag_code = z.principal_diag_code,
    med_visit_diag_code = substr(z.med_visit_diag_code, 1, 20),
    modifier_1_code = z.modifier_1_code,
    modifier_2_code = z.modifier_2_code,
    modifier_3_code = z.modifier_3_code,
    modifier_4_code = z.modifier_4_code,
    modifier_5_code = z.modifier_5_code,
    drug_304b_dc_ind = z.drug_304b_dc_ind,
    pkg_ind = z.pkg_ind,
    pkg_per_diem_ind = z.pkg_per_diem_ind,
    repeat_ancl_dc_ind = z.repeat_ancl_dc_ind,
    sgnf_clinc_proc_csdt_ind = z.sgnf_clinc_proc_csdt_ind,
    sgnf_proc_csdt_ind = z.sgnf_proc_csdt_ind,
    sgnf_proc_dc_cand_ind = z.sgnf_proc_dc_cand_ind,
    termn_proc_dc_ind = z.termn_proc_dc_ind,
    visit_preventive_med_diag_ind = z.visit_preventive_med_diag_ind,
    charge_claim_ind = z.charge_claim_ind,
    charge_ind_code = z.charge_ind_code,
    eapg_bilateral_discount_code = z.eapg_bilateral_discount_code,
    secn_diag_used_code_desc = z.secn_diag_used_code_desc,
    used_visit_diag_code_desc = z.used_visit_diag_code_desc,
    eapg_visit_type_code = z.eapg_visit_type_code,
    charge_code_desc = z.charge_code_desc,
    total_charge_amt = z.total_charge_amt,
    total_covered_charge_amt = z.total_covered_charge_amt,
    non_cov_charge_rank_1_amt = z.non_cov_charge_rank_1_amt,
    non_cov_charge_rank_2_amt = z.non_cov_charge_rank_2_amt,
    non_cov_charge_rank_3_amt = z.non_cov_charge_rank_3_amt,
    non_cov_rank_1_qty = z.non_cov_rank_1_qty,
    non_cov_rank_2_qty = z.non_cov_rank_2_qty,
    non_cov_rank_3_qty = z.non_cov_rank_3_qty,
    visit_cnt = z.visit_cnt,
    proc_code_used_qty = z.proc_code_used_qty,
    secn_diag_code_used_qty = z.secn_diag_code_used_qty,
    service_unit_qty = z.service_unit_qty,
    visit_line_qty = z.visit_line_qty,
    charge_posted_date_time = z.charge_posted_date_time,
    visit_id = z.visit_id,
    calc_method_id = z.calc_method_id,
    calc_profile_id = z.calc_profile_id,
    icd_version_desc = z.icd_version_desc,
    code_type = z.code_type,
    creation_date_time = z.creation_date_time,
    eapg_action_code = z.eapg_action_code,
    jw_modifier_ind = z.jw_modifier_ind,
    dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND),
    source_system_code = 'N' WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_dw_id,
        apg_grouper_num,
        company_code,
        coid,
        unit_num,
        pat_acct_num,
        service_date,
        eapg_code,
        eapg_type_code,
        eapg_category_code,
        eapg_code_version_text,
        revenue_code,
        eapg_unassigned_code,
        used_proc_code,
        principal_diag_code,
        med_visit_diag_code,
        modifier_1_code,
        modifier_2_code,
        modifier_3_code,
        modifier_4_code,
        modifier_5_code,
        drug_304b_dc_ind,
        pkg_ind,
        pkg_per_diem_ind,
        repeat_ancl_dc_ind,
        sgnf_clinc_proc_csdt_ind,
        sgnf_proc_csdt_ind,
        sgnf_proc_dc_cand_ind,
        termn_proc_dc_ind,
        visit_preventive_med_diag_ind,
        charge_claim_ind,
        charge_ind_code,
        eapg_bilateral_discount_code,
        secn_diag_used_code_desc,
        used_visit_diag_code_desc,
        eapg_visit_type_code,
        charge_code_desc,
        total_charge_amt,
        total_covered_charge_amt,
        non_cov_charge_rank_1_amt,
        non_cov_charge_rank_2_amt,
        non_cov_charge_rank_3_amt,
        non_cov_rank_1_qty,
        non_cov_rank_2_qty,
        non_cov_rank_3_qty,
        visit_cnt,
        proc_code_used_qty,
        secn_diag_code_used_qty,
        service_unit_qty,
        visit_line_qty,
        charge_posted_date_time,
        visit_id,
        calc_method_id,
        calc_profile_id,
        icd_version_desc,
        code_type,
        creation_date_time,
        eapg_action_code,
        jw_modifier_ind,
        dw_last_update_date_time,
        source_system_code)
VALUES (z.patient_dw_id, z.apg_grouper_num, z.company_code, z.coid, z.unit_num, z.pat_acct_num, z.service_date, z.eapg_code, z.eapg_type_code, z.eapg_category_code, z.eapg_code_version_text, z.revenue_code, z.eapg_unassigned_code, z.used_proc_code, z.principal_diag_code, substr(z.med_visit_diag_code, 1, 20), z.modifier_1_code, z.modifier_2_code, z.modifier_3_code, z.modifier_4_code, z.modifier_5_code, z.drug_304b_dc_ind, z.pkg_ind, z.pkg_per_diem_ind, z.repeat_ancl_dc_ind, z.sgnf_clinc_proc_csdt_ind, z.sgnf_proc_csdt_ind, z.sgnf_proc_dc_cand_ind, z.termn_proc_dc_ind, z.visit_preventive_med_diag_ind, z.charge_claim_ind, z.charge_ind_code, z.eapg_bilateral_discount_code, z.secn_diag_used_code_desc, z.used_visit_diag_code_desc, z.eapg_visit_type_code, z.charge_code_desc, z.total_charge_amt, z.total_covered_charge_amt, z.non_cov_charge_rank_1_amt, z.non_cov_charge_rank_2_amt, z.non_cov_charge_rank_3_amt, z.non_cov_rank_1_qty, z.non_cov_rank_2_qty, z.non_cov_rank_3_qty, z.visit_cnt, z.proc_code_used_qty, z.secn_diag_code_used_qty, z.service_unit_qty, z.visit_line_qty, z.charge_posted_date_time, z.visit_id, z.calc_method_id, z.calc_profile_id, z.icd_version_desc, z.code_type, z.creation_date_time, z.eapg_action_code, z.jw_modifier_ind, z.dw_last_update_date_time, z.source_system_code);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_dw_id,
             apg_grouper_num
      FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_grouper
      GROUP BY patient_dw_id,
               apg_grouper_num
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_grouper');

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
FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_grouper
WHERE upper(cc_eor_eapg_grouper.coid) IN
    (SELECT upper(r.coid) AS coid
     FROM {{ params.param_parallon_ra_core_dataset_name }}.ref_cc_org_structure AS r
     WHERE upper(rtrim(r.org_status)) = 'ACTIVE' )
  AND cc_eor_eapg_grouper.dw_last_update_date_time <>
    (SELECT max(cc_eor_eapg_grouper_0.dw_last_update_date_time)
     FROM {{ params.param_parallon_ra_core_dataset_name }}.cc_eor_eapg_grouper AS cc_eor_eapg_grouper_0);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL dbadmin_procs.collect_stats_table('edwra','CC_EOR_EAPG_Grouper');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN TRUNCATE TABLE {{ params.param_parallon_ra_stage_dataset_name }}.cc_eor_eapg_grouper_stg;


EXCEPTION WHEN ERROR THEN BEGIN /* Empty. */ END;

END;