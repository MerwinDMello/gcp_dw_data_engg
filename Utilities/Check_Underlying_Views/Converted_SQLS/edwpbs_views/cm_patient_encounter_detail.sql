-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/cm_patient_encounter_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.cm_patient_encounter_detail
   OPTIONS(description='This table contains the information within the Case Management area related to a patient encounter for revenue cycle monitoring.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.midas_encounter_id,
      a.company_code,
      a.coid,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.midas_acct_num,
      ROUND(a.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      a.iplan_id_ins1,
      a.clinical_ed_greet_date,
      a.clinical_ed_greet_time,
      a.clinical_ed_bed_date,
      a.clinical_ed_bed_time,
      a.clinical_ed_triage_date,
      a.clinical_ed_triage_time,
      a.admission_eff_from_date,
      a.total_midnight_cnt,
      a.all_days_approved_ind,
      a.midas_principal_payer_auth_num,
      a.midas_auth_denied_pending_ind,
      a.midas_principal_payer_auth_type_desc,
      a.midas_date_of_denial,
      a.denial_in_midas_status_desc,
      a.midas_avoid_denial_day_documented_ind,
      a.iq_adm_initial_rev_date_time,
      a.iq_adm_reviewer_name,
      a.iq_adm_reviewer_id,
      a.iq_adm_rev_criteria_status,
      a.iq_adm_rev_location,
      a.iq_adm_rev_type_ip_ind,
      a.iq_adm_rev_type_ip_med_necs_met_ind,
      a.iq_adm_rev_type_ip_prior_dchg_ind,
      a.iq_adm_rev_type_obs_ind,
      a.iq_adm_rev_type_obs_med_necs_met_ind,
      a.iq_adm_rev_type_obs_prior_dchg_ind,
      a.iq_adm_rev_med_necs_desc,
      a.cncr_review_cnt,
      a.cncr_iq_cnt,
      a.last_cncr_reviewer_name,
      a.last_cncr_reviewer_id,
      a.last_cncr_review_date,
      a.last_cncr_review_disp_desc,
      a.last_cncr_iq_reviewer_name,
      a.last_cncr_iq_reviewer_id,
      a.last_iq_review_code,
      a.last_iq_review_criteria_met_desc,
      a.last_iq_review_version_desc,
      a.last_iq_review_subset_desc,
      a.last_primary_review_start_date_time,
      a.last_appeal_num,
      a.last_appeal_date,
      a.last_appeal_status_id,
      a.last_appeal_status_name,
      a.peer_review_status_code,
      a.cm_last_no_ins_code_appl_date,
      a.cm_no_ins_ind,
      a.cm_last_svty_illness_code_appl_date,
      a.cm_svty_illness_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.cm_patient_encounter_detail AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
