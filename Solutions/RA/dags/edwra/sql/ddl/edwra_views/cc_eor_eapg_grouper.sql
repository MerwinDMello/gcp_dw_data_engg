-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_eor_eapg_grouper.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_eor_eapg_grouper
   OPTIONS(description='Concuity grouper output for enhanced all payer grouping or EAPG.')
  AS SELECT
      a.patient_dw_id,
      a.apg_grouper_num,
      a.action_code,
      a.calc_method_id,
      a.calc_profile_id,
      a.charge_claim_ind,
      a.charge_code_desc,
      a.charge_ind_code,
      a.charge_posted_date_time,
      a.code_type,
      a.coid,
      a.company_code,
      a.creation_date_time,
      a.drug_304b_dc_ind,
      a.eapg_action_code,
      a.eapg_bilateral_discount_code,
      a.eapg_category_code,
      a.eapg_code,
      a.eapg_code_version_text,
      a.eapg_type_code,
      a.eapg_unassigned_code,
      a.eapg_visit_type_code,
      a.icd_version_desc,
      a.jw_modifier_ind,
      a.med_visit_diag_code,
      a.modifier_1_code,
      a.modifier_2_code,
      a.modifier_3_code,
      a.modifier_4_code,
      a.modifier_5_code,
      a.non_cov_charge_rank_1_amt,
      a.non_cov_charge_rank_2_amt,
      a.non_cov_charge_rank_3_amt,
      a.non_cov_rank_1_qty,
      a.non_cov_rank_2_qty,
      a.non_cov_rank_3_qty,
      a.pat_acct_num,
      a.pkg_ind,
      a.pkg_per_diem_ind,
      a.principal_diag_code,
      a.proc_code_used_qty,
      a.repeat_ancl_dc_ind,
      a.revenue_code,
      a.secn_diag_code_used_qty,
      a.secn_diag_used_code_desc,
      a.service_unit_qty,
      a.service_date,
      a.sgnf_clinc_proc_csdt_ind,
      a.sgnf_proc_csdt_ind,
      a.sgnf_proc_dc_cand_ind,
      a.termn_proc_dc_ind,
      a.total_charge_amt,
      a.total_covered_charge_amt,
      a.unit_num,
      a.used_proc_code,
      a.used_visit_diag_code_desc,
      a.visit_cnt,
      a.visit_id,
      a.visit_line_qty,
      a.visit_preventive_med_diag_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_parallon_ra_base_views_dataset_name }}.cc_eor_eapg_grouper AS a
      INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
       AND rtrim(a.coid) = rtrim(b.co_id)
       AND rtrim(b.user_id) = rtrim(session_user())
  ;
