-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/edr_export_inc016578499.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.edr_export_inc016578499 AS WITH ec1_dy1_dyi1_app1_apps1_e AS (
  SELECT
      row_number() OVER () AS rn,
      ec1.*,
      dy1.*,
      dyi1.*,
      app1.*,
      apps1.*,
      e.*
    FROM
      `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS ec1
      INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
       AND dy1.company_code = ec1.company_code
       AND dy1.active_dw_ind = 'Y'
      INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
       AND upper(dyi1.active_dw_ind) = 'Y'
      INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_appeal AS app1 ON app1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
       AND app1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
       AND app1.active_dw_ind = 'Y'
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_appeal_status AS apps1 ON apps1.hcm_appeal_status_id = app1.hcm_appeal_status_id
       AND upper(apps1.active_dw_ind) = 'Y'
      LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS e ON e.empl_id = app1.system_user_id
       AND upper(e.active_dw_ind) = 'Y'
), ec1_dy1_dyi1_app1_apps1_e_e1 AS (
  SELECT
      ec1_dy1_dyi1_app1_apps1_e.rn,
      ec1_dy1_dyi1_app1_apps1_e.midas_encounter_id,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date,
      ec1_dy1_dyi1_app1_apps1_e.patient_dw_id,
      ec1_dy1_dyi1_app1_apps1_e.company_code,
      ec1_dy1_dyi1_app1_apps1_e.coid,
      ec1_dy1_dyi1_app1_apps1_e.midas_facility_code,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date,
      ec1_dy1_dyi1_app1_apps1_e.location_id,
      ec1_dy1_dyi1_app1_apps1_e.admitting_service_id,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_initial_review_empl_id,
      ec1_dy1_dyi1_app1_apps1_e.pat_acct_num,
      ec1_dy1_dyi1_app1_apps1_e.total_review_cnt,
      ec1_dy1_dyi1_app1_apps1_e.completed_review_cnt,
      ec1_dy1_dyi1_app1_apps1_e.midas_acct_num,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind,
      ec1_dy1_dyi1_app1_apps1_e.admit_weekend_ind,
      ec1_dy1_dyi1_app1_apps1_e.pdu_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_ip_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_obs_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_ip_ptd_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_obs_ptd_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_ip_mn_met_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_type_obs_mn_met_ind,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_initial_review_hour,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_initial_rev_date_time,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_criteria_status,
      ec1_dy1_dyi1_app1_apps1_e.iq_adm_rev_location_id,
      ec1_dy1_dyi1_app1_apps1_e.midas_encounter_last_updt_date,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_cm_ind,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_apel_ind,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_other_ind,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_cm_cnt,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_apel_cnt,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_other_cnt,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_disp_id,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_date_time,
      ec1_dy1_dyi1_app1_apps1_e.external_pa_referral_id,
      ec1_dy1_dyi1_app1_apps1_e.midas_last_ip_encounter_id,
      ec1_dy1_dyi1_app1_apps1_e.denial_onbase_unique_id,
      ec1_dy1_dyi1_app1_apps1_e.document_date,
      ec1_dy1_dyi1_app1_apps1_e.bpci_episode_group_id,
      ec1_dy1_dyi1_app1_apps1_e.bpci_data_science_percentage_num,
      ec1_dy1_dyi1_app1_apps1_e.bpci_data_science_date,
      ec1_dy1_dyi1_app1_apps1_e.concurrent_denial_code,
      ec1_dy1_dyi1_app1_apps1_e.inpatient_admit_review_cnt,
      ec1_dy1_dyi1_app1_apps1_e.first_inpatient_admit_review_date,
      ec1_dy1_dyi1_app1_apps1_e.last_inpatient_admit_review_date,
      ec1_dy1_dyi1_app1_apps1_e.inpatient_admit_review_delay_day_cnt,
      ec1_dy1_dyi1_app1_apps1_e.inpatient_admit_review_1_day_delay_ind,
      ec1_dy1_dyi1_app1_apps1_e.obs_prog_rev_after_disch_ind,
      ec1_dy1_dyi1_app1_apps1_e.go_live_date_ind,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_id,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date AS eff_from_date_1,
      ec1_dy1_dyi1_app1_apps1_e.patient_dw_id AS patient_dw_id_1,
      ec1_dy1_dyi1_app1_apps1_e.company_code AS company_code_1,
      ec1_dy1_dyi1_app1_apps1_e.coid AS coid_1,
      ec1_dy1_dyi1_app1_apps1_e.concurrent_review_id,
      ec1_dy1_dyi1_app1_apps1_e.update_by_user_id,
      ec1_dy1_dyi1_app1_apps1_e.reimbursement_group_code,
      ec1_dy1_dyi1_app1_apps1_e.reimbursement_group_name,
      ec1_dy1_dyi1_app1_apps1_e.reimbursement_group_start_date,
      ec1_dy1_dyi1_app1_apps1_e.midas_encounter_id AS midas_encounter_id_1,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_ext_id,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_last_update,
      ec1_dy1_dyi1_app1_apps1_e.tracer_comment,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date AS eff_to_date_1,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind AS initial_record_ind_1,
      ec1_dy1_dyi1_app1_apps1_e.pdu_ind AS pdu_ind_1,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code AS source_system_code_1,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind AS active_dw_ind_1,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time AS dw_last_update_date_time_1,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_info_id,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_id AS hcm_avoid_denied_day_id_1,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date AS eff_from_date_1_0,
      ec1_dy1_dyi1_app1_apps1_e.entered_by_user_id,
      ec1_dy1_dyi1_app1_apps1_e.authorization_num,
      ec1_dy1_dyi1_app1_apps1_e.avoidable_denied_day_cnt,
      ec1_dy1_dyi1_app1_apps1_e.appeal_closed_date,
      ec1_dy1_dyi1_app1_apps1_e.avoid_denied_start_date,
      ec1_dy1_dyi1_app1_apps1_e.avoid_denied_end_date,
      ec1_dy1_dyi1_app1_apps1_e.date_of_denial,
      ec1_dy1_dyi1_app1_apps1_e.concurrent_denial_type_ind,
      ec1_dy1_dyi1_app1_apps1_e.day_recovered_cnt,
      ec1_dy1_dyi1_app1_apps1_e.dollar_avoidable_denied_amt,
      ec1_dy1_dyi1_app1_apps1_e.dollar_recovered_amt,
      ec1_dy1_dyi1_app1_apps1_e.insurance_num,
      ec1_dy1_dyi1_app1_apps1_e.location_id AS location_id_1,
      ec1_dy1_dyi1_app1_apps1_e.type_of_day_id,
      ec1_dy1_dyi1_app1_apps1_e.type_of_day_recovered_id,
      ec1_dy1_dyi1_app1_apps1_e.payor_avoid_denied_day_id,
      ec1_dy1_dyi1_app1_apps1_e.payor_avoid_denied_day_code,
      ec1_dy1_dyi1_app1_apps1_e.payor_avoid_denied_day_name,
      ec1_dy1_dyi1_app1_apps1_e.total_department_day_cnt,
      ec1_dy1_dyi1_app1_apps1_e.total_physician_day_cnt,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date AS eff_to_date_1_0,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind AS initial_record_ind_1_0,
      ec1_dy1_dyi1_app1_apps1_e.pdu_ind AS pdu_ind_1_0,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code AS source_system_code_1_0,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind AS active_dw_ind_1_0,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time AS dw_last_update_date_time_1_0,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_apel_id,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_info_id AS hcm_avoid_denied_day_info_id_1,
      ec1_dy1_dyi1_app1_apps1_e.hcm_avoid_denied_day_id AS hcm_avoid_denied_day_id_1_0,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date AS eff_from_date_1_1,
      ec1_dy1_dyi1_app1_apps1_e.hcm_appeal_status_id,
      ec1_dy1_dyi1_app1_apps1_e.date_of_appeal,
      ec1_dy1_dyi1_app1_apps1_e.system_user_id,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date AS eff_to_date_1_1,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind AS initial_record_ind_1_1,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code AS source_system_code_1_1,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind AS active_dw_ind_1_1,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time AS dw_last_update_date_time_1_1,
      ec1_dy1_dyi1_app1_apps1_e.hcm_appeal_status_id AS hcm_appeal_status_id_1,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date AS eff_from_date_1_2,
      ec1_dy1_dyi1_app1_apps1_e.hcm_appeal_status_code,
      ec1_dy1_dyi1_app1_apps1_e.hcm_appeal_status_name,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date AS eff_to_date_1_2,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind AS initial_record_ind_1_2,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code AS source_system_code_1_2,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind AS active_dw_ind_1_2,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time AS dw_last_update_date_time_1_2,
      ec1_dy1_dyi1_app1_apps1_e.empl_id,
      ec1_dy1_dyi1_app1_apps1_e.eff_from_date AS eff_from_date_1_3,
      ec1_dy1_dyi1_app1_apps1_e.empl_num,
      ec1_dy1_dyi1_app1_apps1_e.empl_first_last_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_first_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_last_first_middle_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_last_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_middle_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_first_middle_last_name,
      ec1_dy1_dyi1_app1_apps1_e.empl_name,
      ec1_dy1_dyi1_app1_apps1_e.birthdate,
      ec1_dy1_dyi1_app1_apps1_e.initial_hire_date,
      ec1_dy1_dyi1_app1_apps1_e.termination_date,
      ec1_dy1_dyi1_app1_apps1_e.sex_code,
      ec1_dy1_dyi1_app1_apps1_e.eff_to_date AS eff_to_date_1_3,
      ec1_dy1_dyi1_app1_apps1_e.initial_record_ind AS initial_record_ind_1_3,
      ec1_dy1_dyi1_app1_apps1_e.source_system_code AS source_system_code_1_3,
      ec1_dy1_dyi1_app1_apps1_e.active_dw_ind AS active_dw_ind_1_3,
      ec1_dy1_dyi1_app1_apps1_e.dw_last_update_date_time AS dw_last_update_date_time_1_3,
      e1.empl_id AS empl_id_1,
      e1.eff_from_date AS eff_from_date_1_4,
      e1.empl_num AS empl_num_1,
      e1.empl_first_last_name AS empl_first_last_name_1,
      e1.empl_first_name AS empl_first_name_1,
      e1.empl_last_first_middle_name AS empl_last_first_middle_name_1,
      e1.empl_last_name AS empl_last_name_1,
      e1.empl_middle_name AS empl_middle_name_1,
      e1.empl_first_middle_last_name AS empl_first_middle_last_name_1,
      e1.empl_name AS empl_name_1,
      e1.birthdate AS birthdate_1,
      e1.initial_hire_date AS initial_hire_date_1,
      e1.termination_date AS termination_date_1,
      e1.sex_code AS sex_code_1,
      e1.eff_to_date AS eff_to_date_1_4,
      e1.initial_record_ind AS initial_record_ind_1_4,
      e1.source_system_code AS source_system_code_1_4,
      e1.active_dw_ind AS active_dw_ind_1_4,
      e1.dw_last_update_date_time AS dw_last_update_date_time_1_4
    FROM
      ec1_dy1_dyi1_app1_apps1_e
      INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS e1 ON e1.empl_id = ec1_dy1_dyi1_app1_apps1_e.system_user_id
       AND upper(e1.active_dw_ind) = 'N'
    WHERE e1.dw_last_update_date_time = (
      SELECT
          max(emax.dw_last_update_date_time)
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS emax
        WHERE upper(emax.active_dw_ind) = 'N'
         AND emax.empl_id = e1.empl_id
    )
), e1 AS (
  SELECT
      ec1_dy1_dyi1_app1_apps1_e.*,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_0,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_1,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_2,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_3,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_4,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_5,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_6,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_7,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_8,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_9,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_10,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_11,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_12,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_13,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_14,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_15,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_16,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_17,
      CAST(NULL as ERROR_TYPE(Error<error-type>)) AS null_18
    FROM
      ec1_dy1_dyi1_app1_apps1_e
    WHERE ec1_dy1_dyi1_app1_apps1_e.rn NOT IN(
      SELECT
          ec1_dy1_dyi1_app1_apps1_e_e1.rn AS rn
        FROM
          ec1_dy1_dyi1_app1_apps1_e_e1
    )
)
SELECT
    org.concuity_schema AS concuity_schema,
    ccap.unit_num AS unit_num,
    ccap.pat_acct_num AS pat_acct_num,
    ccap.iplan_id AS iplan_id,
    ccap.iplan_order_num AS iplan_order_num,
    esl.esl_level_1_desc AS esl_level_1_desc,
    esl.esl_level_2_desc AS esl_level_2_desc,
    esl.esl_level_3_desc AS esl_level_3_desc,
    esl.esl_level_4_desc AS esl_level_4_desc,
    esl.esl_level_5_desc AS esl_level_5_desc,
    drg_pa.chois_product_line_code AS chois_product_line_code,
    drg_pa.chois_product_line_desc AS chois_product_line_desc,
    drg_pa.drg_medical_surgical_ind AS drg_medical_surgical_ind,
    apr.apr_drg_code AS apr_drg_code,
    apr.apr_drg_grouper_name AS apr_drg_grouper_name,
    apr.apr_severity_of_illness_desc AS apr_severity_of_illness_desc,
    apr.apr_risk_of_mortality_desc AS apr_risk_of_mortality_desc,
    fi.payer_type_code AS payer_type_code,
    fi.sub_payor_group_id AS sub_payor_group_id,
    coalesce(condcode.cond_code_xf_xg_ind, 'N') AS cond_code_xf_xg_ind,
    coalesce(condcode.cond_code_nu_ind, 'N') AS cond_code_nu_ind,
    coalesce(condcode.cond_code_ne_ind, 'N') AS cond_code_ne_ind,
    coalesce(condcode.cond_code_ns_ind, 'N') AS cond_code_ns_ind,
    coalesce(condcode.cond_code_np_ind, 'N') AS cond_code_np_ind,
    coalesce(condcode.cond_code_no_ind, 'N') AS cond_code_no_ind,
    ri.treatment_authorization_num AS treatment_authorization_num,
    cmd.denial_in_midas_status AS denial_in_midas_status,
    cmd.midas_date_of_denial AS midas_date_of_denial,
    coalesce(cmd.all_days_approved_ind, 'N') AS all_days_approved_ind,
    urs.ptp_performed_ind AS ptp_performed,
    coalesce(cmd.cm_xf_ind, 'N') AS cm_xf_ind,
    coalesce(cmd.cm_xg_ind, 'N') AS cm_xg_ind,
    cmd.cm_last_xf_code_applied_date AS cm_last_xf_code_applied_date,
    cmd.cm_last_xg_code_applied_date AS cm_last_xg_code_applied_date,
    cmd.midas_acct_num AS midas_acct_num,
    cmd.last_appeal_date AS last_appeal_date,
    cmd.last_appeal_status AS last_appeal_status,
    cmd.last_appeal_employee_id AS last_appeal_employee_id,
    cmd.last_appeal_employee_name AS last_appeal_employee_name,
    cmd.status_cause_name AS status_cause_name,
    cmd.last_conc_review_disp AS last_conc_review_disp,
    cmd.midas_principal_payer_auth_num AS midas_principal_payer_auth_num,
    cmd.midas_principal_payer_auth_type AS midas_principal_pyr_auth_type,
    cmd.cm_last_iq_review_criteria_met_desc AS cm_last_iq_revi_crit_met_desc,
    cmd.cm_last_iq_review_version_desc AS cm_last_iq_review_version_desc,
    cmd.cm_last_iq_review_subset_desc AS cm_last_iq_review_subset_desc,
    pdu.pdu_determination_reason_desc AS pdu_determination_reason_desc,
    pa.ins1_payor_balance_amt AS ins1_payor_balance_amt,
    pa.ins2_payor_balance_amt AS ins2_payor_balance_amt,
    pa.ins3_payor_balance_amt AS ins3_payor_balance_amt,
    prepay.min_doc_req_medrec_request_date AS first_doc_request_mr_date,
    prepay.max_doc_req_medrec_request_date AS last_doc_request_mr_date,
    prepay.min_doc_req_medrec_sent_date AS first_doc_sent_mr_date,
    prepay.max_doc_req_medrec_sent_date AS last_doc_sent_mr_date,
    prepay.min_doc_req_ib_request_date AS first_doc_request_ib_date,
    prepay.max_doc_req_ib_request_date AS last_doc_request_ib_date,
    prepay.min_doc_req_ib_sent_date AS first_doc_sent_ib_date,
    prepay.max_doc_req_ib_sent_date AS last_doc_sent_ib_date,
    prepay.first_doc_request_date AS first_doc_request_date,
    prepay.last_doc_request_date AS last_doc_request_date,
    prepay.first_doc_sent_date AS first_doc_sent_date,
    prepay.last_doc_sent_date AS last_doc_sent_date,
    prepay.first_doc_received_date AS first_doc_received_date,
    prepay.last_doc_received_date AS last_doc_received_date,
    prepay.first_doc_approved_date AS first_doc_approved_date,
    prepay.last_doc_approved_date AS last_doc_approved_date,
    prepay.first_doc_denied_date AS first_doc_denied_date,
    prepay.last_doc_denied_date AS last_doc_denied_date,
    coalesce(CASE
      WHEN cov.patient_dw_id IS NOT NULL THEN 'Y'
      ELSE 'N'
    END, 'N') AS covid_positive_flag,
    cr1py.refund_amt AS refund_amt,
    cr1py.entered_date AS refund_create_date,
    cr1py.refund_creation_user_id AS refund_requested_by,
    cr1pt.refund_amt AS patient_refund_amt,
    cr1pt.entered_date AS patient_refund_create_date,
    cr1pt.refund_creation_user_id AS patient_refund_requested_by,
    crb.credit_status_alias AS credit_status,
    msccr.discrepancy_source_desc AS discrepancy_source_desc,
    msccr.reimbursement_impact_desc AS reimbursement_impact_desc,
    msccr.discrepancy_date_time AS discrepancy_date_time,
    msccr.request_date_time AS request_date_time,
    msccr.reprocess_reason_text AS reprocess_reason_text,
    msccr.status_desc AS status_desc,
    coalesce(splitbill.split_bill_ind, 'N') AS split_bill_ind,
    scr.last_scripted_applied_date_time AS last_scrted_appl_date_time,
    scr.scripted_overpayment_desc AS scripted_overpayment_desc,
    ltr.last_letter_sent_date_time AS last_letter_sent_date_time,
    cca.account_id AS account_id,
    ccap.account_payor_id AS account_payor_id,
    ccorg.org_id AS org_id
  FROM
    `hca-hin-dev-cur-parallon`.edwra_views.cc_account_payor AS ccap
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwra_views.cc_account AS cca ON cca.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwra_views.ref_cc_org_structure AS ccorg ON ccorg.company_code = ccap.company_code
     AND ccorg.coid = ccap.coid
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp ON fp.patient_dw_id = ccap.patient_dw_id
     AND fp.company_code = ccap.company_code
     AND fp.coid = ccap.coid
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_detail_lvl AS pa ON pa.patient_dw_id = ccap.patient_dw_id
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS fi ON fi.payor_dw_id = ccap.payor_dw_id
     AND fi.company_code = ccap.company_code
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.patient_esl_detail AS esl ON esl.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_organization AS org ON org.coid = ccap.coid
     AND org.company_code = ccap.company_code
    LEFT OUTER JOIN (
      SELECT
          dg.reimbursement_group_code,
          dg.reimbursement_group_start_date,
          dg.reimbursement_group_end_date,
          dg.drg_medical_surgical_ind,
          dg.chois_product_line_code,
          dg.chois_product_line_desc
        FROM
          `hca-hin-dev-cur-parallon`.edw_pub_views.diagnosis_related_group AS dg
        WHERE dg.reimbursement_group_name = 'M'
    ) AS drg_pa ON drg_pa.reimbursement_group_code = fp.drg_hcfa_icd10_code
     AND fp.discharge_date BETWEEN drg_pa.reimbursement_group_start_date AND drg_pa.reimbursement_group_end_date
    LEFT OUTER JOIN (
      SELECT
          a.patient_dw_id,
          a.coid,
          a.company_code,
          a.drg_code AS apr_drg_code,
          a.drg_grouper_name AS apr_drg_grouper_name,
          si.severity_of_illness_desc AS apr_severity_of_illness_desc,
          rm.risk_of_mortality_desc AS apr_risk_of_mortality_desc
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.admission_drg AS a
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.ref_apr_severity_of_illness AS si ON trim(si.apr_severity_of_illness) = trim(a.apr_severity_of_illness)
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.ref_apr_risk_of_mortality AS rm ON trim(rm.apr_risk_of_mortality) = trim(a.apr_risk_of_mortality)
        WHERE upper(a.company_code) = 'H'
         AND upper(a.drg_usage_code) = 'A'
         AND a.icd_version_ind = 0
        QUALIFY row_number() OVER (PARTITION BY a.patient_dw_id ORDER BY a.pa_last_update_date DESC) = 1
    ) AS apr ON apr.patient_dw_id = ccap.patient_dw_id
     AND apr.coid = ccap.coid
     AND apr.company_code = ccap.company_code
    LEFT OUTER JOIN (
      SELECT
          patient_condition_code.patient_dw_id,
          max(CASE
            WHEN patient_condition_code.condition_code IN(
              'XF', 'XG'
            ) THEN 'Y'
            ELSE 'N'
          END) AS cond_code_xf_xg_ind,
          max(CASE
            WHEN patient_condition_code.condition_code = 'NU' THEN 'Y'
            ELSE 'N'
          END) AS cond_code_nu_ind,
          max(CASE
            WHEN patient_condition_code.condition_code = 'NE' THEN 'Y'
            ELSE 'N'
          END) AS cond_code_ne_ind,
          max(CASE
            WHEN patient_condition_code.condition_code = 'NS' THEN 'Y'
            ELSE 'N'
          END) AS cond_code_ns_ind,
          max(CASE
            WHEN patient_condition_code.condition_code = 'NP' THEN 'Y'
            ELSE 'N'
          END) AS cond_code_np_ind,
          max(CASE
            WHEN patient_condition_code.condition_code = 'NO' THEN 'Y'
            ELSE 'N'
          END) AS cond_code_no_ind
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.patient_condition_code
        WHERE patient_condition_code.condition_code IN(
          'XF', 'XG', 'NU', 'NE', 'NS', 'NP', 'NO'
        )
        GROUP BY 1
        QUALIFY row_number() OVER (PARTITION BY patient_condition_code.patient_dw_id ORDER BY patient_condition_code.patient_dw_id) = 1
    ) AS condcode ON condcode.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.registration_iplan_pf AS ri ON ri.patient_dw_id = ccap.patient_dw_id
     AND ri.payor_dw_id = ccap.payor_dw_id
     AND ri.iplan_insurance_order_num = ccap.iplan_order_num
     AND ri.coid = ccap.coid
     AND ri.company_code = ccap.company_code
     AND ri.iplan_id = ccap.iplan_id
     AND upper(ri.eff_to_date) = '9999-12-31'
    LEFT OUTER JOIN (
      SELECT
          fp_0.company_code,
          fp_0.patient_dw_id,
          u.acct_with_denial_ind,
          CASE
             1
            WHEN u.concurrent_overturned_peer_to_peer_ind THEN 'Y - Overturned'
            WHEN u.post_dchg_overturned_peer_to_peer_ind THEN 'Y - Overturned'
            WHEN u.concurrent_upheld_peer_to_peer_ind THEN 'Y - Upheld'
            WHEN u.post_dchg_upheld_peer_to_peer_ind THEN 'Y - Upheld'
            ELSE 'N - Not Performed'
          END AS ptp_performed_ind
        FROM
          `hca-hin-dev-cur-parallon`.edwcm_stnd_views.ur_metric AS u
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp_0 ON fp_0.patient_dw_id = u.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_facility AS ff ON ff.coid = fp_0.coid
           AND ff.company_code = fp_0.company_code
        WHERE fp_0.company_code = 'H'
         AND upper(ff.coid_status_code) = 'F'
         AND u.well_baby_ind = 'N'
         AND fp_0.admission_patient_type_code LIKE '%I%'
         AND (u.acct_with_denial_ind = 1
         OR u.concurrent_overturned_peer_to_peer_ind = 1
         OR u.post_dchg_overturned_peer_to_peer_ind = 1
         OR u.concurrent_upheld_peer_to_peer_ind = 1
         OR u.post_dchg_upheld_peer_to_peer_ind = 1)
    ) AS urs ON urs.company_code = ccap.company_code
     AND urs.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          p.patient_dw_id,
          p.coid,
          p.unit_num,
          p.pat_acct_num,
          e.midas_acct_num,
          p.payor_dw_id_ins1,
          p.iplan_id_ins1,
          CASE
            WHEN dend.midas_encounter_id IS NOT NULL
             AND dod.date_of_denial IS NULL THEN 'Denial in Midas (No DNL Date)'
            WHEN dend.midas_encounter_id IS NOT NULL
             AND dod.date_of_denial > p.discharge_date THEN 'Denial in Midas (Post Disch)'
            WHEN dend.midas_encounter_id IS NOT NULL
             AND dod.date_of_denial <= p.discharge_date THEN 'Concurrent Denial (Pre Disch)'
            WHEN dend.midas_encounter_id IS NULL
             AND dod.date_of_denial IS NULL THEN 'Not in Midas'
            ELSE 'Other'
          END AS denial_in_midas_status,
          dod.date_of_denial AS midas_date_of_denial,
          CASE
            WHEN fdf.patient_dw_id IS NOT NULL THEN 'Y'
            ELSE 'N'
          END AS all_days_approved_ind,
          CASE
            WHEN ptpp.midas_encounter_id IS NOT NULL
             AND ptpp.hcm_appeal_status_id IN(
              33
            ) THEN 'Y - Overturned'
            WHEN ptpp.midas_encounter_id IS NOT NULL
             AND ptpp.hcm_appeal_status_id IN(
              34, 38
            ) THEN 'Y - Upheld'
            ELSE 'N - Not Performed'
          END AS ptp_performed,
          xc.cm_last_xf_code_applied_date,
          xc.cm_xf_ind,
          xc.cm_last_xg_code_applied_date,
          xc.cm_xg_ind,
          appl.date_of_appeal AS last_appeal_date,
          appl.hcm_appeal_status_name AS last_appeal_status,
          appl.last_appeal_employee_id,
          appl.last_appeal_employee_name,
          rc.hcm_status_cause_name AS status_cause_name,
          miq4.hcm_disposition_desc AS last_conc_review_disp,
          epa.midas_principal_payer_auth_num,
          CASE
            WHEN upper(epa.midas_principal_payer_auth_num) LIKE '%1%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%2%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%3%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%4%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%5%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%6%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%7%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%8%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%9%'
             OR upper(epa.midas_principal_payer_auth_num) LIKE '%0%' THEN CASE
              WHEN epa.midas_principal_payer_auth_num IS NULL THEN 'No Auth #'
              WHEN upper(trim(epa.midas_principal_payer_auth_num)) = '' THEN 'No Auth #'
              WHEN strpos(epa.midas_principal_payer_auth_num, '/') = 0 THEN 'No "/" in Auth #'
              WHEN upper(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2)) = '/I' THEN 'Inpatient (/I)'
              WHEN upper(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2)) = '/V' THEN 'Observation (/V)'
              WHEN upper(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2)) = '/S' THEN 'IP Skilled Nursing or Swing Bed (/S)'
              WHEN upper(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 2)) = '/B' THEN 'IP Behavioral Health (/B)'
              WHEN upper(substr(epa.midas_principal_payer_auth_num, strpos(epa.midas_principal_payer_auth_num, '/'), 4)) = '/CPT' THEN 'Outpatient (/CPT)'
              ELSE 'Other'
            END
            ELSE epa.midas_principal_payer_auth_num
          END AS midas_principal_payer_auth_type,
          miq.iq_review_criteria_met_desc AS cm_last_iq_review_criteria_met_desc,
          miq.interqual_review_version_desc AS cm_last_iq_review_version_desc,
          miq.interqual_review_subset_desc AS cm_last_iq_review_subset_desc
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS p
          INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS e ON e.patient_dw_id = p.patient_dw_id
           AND e.company_code = p.company_code
           AND e.active_dw_ind = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter_payer AS epa ON epa.midas_encounter_id = e.midas_encounter_id
           AND epa.active_dw_ind = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS ep ON ep.empl_id = e.iq_adm_initial_review_empl_id
           AND upper(ep.active_dw_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_cm_location AS lo ON lo.location_id = e.iq_adm_rev_location_id
           AND lo.active_dw_ind = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.fact_facility AS ff ON ff.coid = p.coid
           AND ff.company_code = p.company_code
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_facility AS cmf ON cmf.coid = e.coid
           AND cmf.midas_facility_code = e.midas_facility_code
           AND cmf.company_code = e.company_code
           AND upper(cmf.active_dw_ind) = 'Y'
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.clinical_ed_event_date_time AS cl ON cl.patient_dw_id = e.patient_dw_id
           AND cl.company_code = e.company_code
          LEFT OUTER JOIN (
            SELECT
                apt.patient_dw_id,
                apt.eff_from_date,
                apt.admission_patient_type_code,
                row_number() OVER (PARTITION BY apt.patient_dw_id ORDER BY apt.patient_dw_id, apt.eff_from_date) AS rec_num
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.admission_patient_type AS apt
              WHERE apt.admission_patient_type_code NOT IN(
                'EP ', 'OP ', 'SP ', 'IP ', 'ER ', 'OR ', 'SR ', 'ERV', 'ORV', 'SRV', 'N  '
              )
          ) AS ipt ON ipt.patient_dw_id = p.patient_dw_id
           AND ipt.rec_num = 1
          LEFT OUTER JOIN (
            SELECT
                ed.midas_encounter_id,
                ia.date_of_denial
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS ed
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day AS ds ON ds.midas_encounter_id = ed.midas_encounter_id
                 AND ds.active_dw_ind = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day_info AS ia ON ia.hcm_avoid_denied_day_id = ds.hcm_avoid_denied_day_id
                 AND upper(ia.active_dw_ind) = 'Y'
              WHERE ed.active_dw_ind = 'Y'
               AND ia.date_of_denial IS NOT NULL
              QUALIFY row_number() OVER (PARTITION BY ed.midas_encounter_id ORDER BY ia.date_of_denial) = 1
          ) AS dod ON dod.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT DISTINCT
                cr.midas_encounter_id,
                count(rc_0.conc_rev_conc_rev_id) AS conc_review_cnt,
                count(rc_0.interqual_review_code) AS conc_iq_cnt
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS cr
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS rc_0 ON rc_0.concurrent_review_id = cr.concurrent_review_id
                 AND upper(rc_0.active_dw_ind) = 'Y'
              WHERE cr.active_dw_ind = 'Y'
              GROUP BY 1
          ) AS conccnt ON conccnt.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                ec1.midas_encounter_id,
                app1.hcm_avoid_denied_apel_id,
                app1.date_of_appeal,
                app1.hcm_appeal_status_id,
                apps1.hcm_appeal_status_name
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS ec1
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
                 AND dy1.company_code = ec1.company_code
                 AND dy1.active_dw_ind = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
                 AND upper(dyi1.active_dw_ind) = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_appeal AS app1 ON app1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
                 AND app1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
                 AND app1.active_dw_ind = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_appeal_status AS apps1 ON apps1.hcm_appeal_status_id = app1.hcm_appeal_status_id
                 AND upper(apps1.active_dw_ind) = 'Y'
              WHERE ec1.active_dw_ind = 'Y'
               AND app1.hcm_appeal_status_id IN(
                33, 34, 38
              )
              QUALIFY row_number() OVER (PARTITION BY ec1.midas_encounter_id ORDER BY app1.hcm_avoid_denied_apel_id DESC) = 1
          ) AS ptpp ON ptpp.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                ec1.midas_encounter_id,
                att1.hcm_status_cause_id,
                atts1.hcm_status_cause_name
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS ec1
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day AS dy1 ON dy1.midas_encounter_id = ec1.midas_encounter_id
                 AND dy1.company_code = ec1.company_code
                 AND dy1.active_dw_ind = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day_info AS dyi1 ON dyi1.hcm_avoid_denied_day_id = dy1.hcm_avoid_denied_day_id
                 AND upper(dyi1.active_dw_ind) = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_attribution AS att1 ON att1.hcm_avoid_denied_day_id = dyi1.hcm_avoid_denied_day_id
                 AND att1.hcm_avoid_denied_day_info_id = dyi1.hcm_avoid_denied_day_info_id
                 AND upper(att1.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_status_cause AS atts1 ON atts1.hcm_status_cause_id = att1.hcm_status_cause_id
                 AND atts1.active_dw_ind = 'Y'
              WHERE ec1.active_dw_ind = 'Y'
               AND upper(atts1.hcm_status_cause_name) LIKE 'DEN - %'
               AND att1.hcm_status_cause_id IS NOT NULL
              QUALIFY row_number() OVER (PARTITION BY ec1.midas_encounter_id ORDER BY att1.hcm_avoid_denied_attr_id DESC) = 1
          ) AS rc ON rc.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                ec1_dy1_dyi1_app1_apps1_e_e1_e1.midas_encounter_id,
                ec1_dy1_dyi1_app1_apps1_e_e1_e1.hcm_avoid_denied_apel_id,
                ec1_dy1_dyi1_app1_apps1_e_e1_e1.date_of_appeal,
                ec1_dy1_dyi1_app1_apps1_e_e1_e1.hcm_appeal_status_id,
                ec1_dy1_dyi1_app1_apps1_e_e1_e1.hcm_appeal_status_name,
                coalesce(ec1_dy1_dyi1_app1_apps1_e_e1_e1.empl_num, ec1_dy1_dyi1_app1_apps1_e_e1_e1.empl_num_1) AS last_appeal_employee_id,
                coalesce(ec1_dy1_dyi1_app1_apps1_e_e1_e1.empl_name, ec1_dy1_dyi1_app1_apps1_e_e1_e1.empl_name_1) AS last_appeal_employee_name
              FROM
                (
                  SELECT
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_encounter_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.patient_dw_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.company_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.coid,
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_facility_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.location_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.admitting_service_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_initial_review_empl_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.pat_acct_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.total_review_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.completed_review_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_acct_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.admit_weekend_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.pdu_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_ip_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_obs_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_ip_ptd_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_obs_ptd_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_ip_mn_met_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_type_obs_mn_met_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_initial_review_hour,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_initial_rev_date_time,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_criteria_status,
                      ec1_dy1_dyi1_app1_apps1_e_e1.iq_adm_rev_location_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_encounter_last_updt_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_cm_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_apel_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_other_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_cm_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_apel_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_other_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_disp_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_date_time,
                      ec1_dy1_dyi1_app1_apps1_e_e1.external_pa_referral_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_last_ip_encounter_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.denial_onbase_unique_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.document_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.bpci_episode_group_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.bpci_data_science_percentage_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.bpci_data_science_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.concurrent_denial_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.inpatient_admit_review_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.first_inpatient_admit_review_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.last_inpatient_admit_review_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.inpatient_admit_review_delay_day_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.inpatient_admit_review_1_day_delay_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.obs_prog_rev_after_disch_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.go_live_date_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.patient_dw_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.company_code_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.coid_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.concurrent_review_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.update_by_user_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.reimbursement_group_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.reimbursement_group_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.reimbursement_group_start_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.midas_encounter_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_ext_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_last_update,
                      ec1_dy1_dyi1_app1_apps1_e_e1.tracer_comment,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.pdu_ind_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_info_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1 AS eff_from_date_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.entered_by_user_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.authorization_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.avoidable_denied_day_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.appeal_closed_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.avoid_denied_start_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.avoid_denied_end_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.date_of_denial,
                      ec1_dy1_dyi1_app1_apps1_e_e1.concurrent_denial_type_ind,
                      ec1_dy1_dyi1_app1_apps1_e_e1.day_recovered_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dollar_avoidable_denied_amt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dollar_recovered_amt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.insurance_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.location_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.type_of_day_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.type_of_day_recovered_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.payor_avoid_denied_day_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.payor_avoid_denied_day_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.payor_avoid_denied_day_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.total_department_day_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.total_physician_day_cnt,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1 AS eff_to_date_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1 AS initial_record_ind_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.pdu_ind_1 AS pdu_ind_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1 AS source_system_code_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1 AS active_dw_ind_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1 AS dw_last_update_date_time_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_apel_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_info_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_avoid_denied_day_id_1 AS hcm_avoid_denied_day_id_1_0,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1 AS eff_from_date_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_appeal_status_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.date_of_appeal,
                      ec1_dy1_dyi1_app1_apps1_e_e1.system_user_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1 AS eff_to_date_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1 AS initial_record_ind_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1 AS source_system_code_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1 AS active_dw_ind_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1 AS dw_last_update_date_time_1_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_appeal_status_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1 AS eff_from_date_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_appeal_status_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.hcm_appeal_status_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1 AS eff_to_date_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1 AS initial_record_ind_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1 AS source_system_code_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1 AS active_dw_ind_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1 AS dw_last_update_date_time_1_2,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_id,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1 AS eff_from_date_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_num,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_last_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_last_first_middle_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_last_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_middle_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_middle_last_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_name,
                      ec1_dy1_dyi1_app1_apps1_e_e1.birthdate,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_hire_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.termination_date,
                      ec1_dy1_dyi1_app1_apps1_e_e1.sex_code,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1 AS eff_to_date_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1 AS initial_record_ind_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1 AS source_system_code_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1 AS active_dw_ind_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1 AS dw_last_update_date_time_1_3,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_id_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_from_date_1 AS eff_from_date_1_4,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_num_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_last_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_last_first_middle_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_last_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_middle_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_first_middle_last_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.empl_name_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.birthdate_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_hire_date_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.termination_date_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.sex_code_1,
                      ec1_dy1_dyi1_app1_apps1_e_e1.eff_to_date_1 AS eff_to_date_1_4,
                      ec1_dy1_dyi1_app1_apps1_e_e1.initial_record_ind_1 AS initial_record_ind_1_4,
                      ec1_dy1_dyi1_app1_apps1_e_e1.source_system_code_1 AS source_system_code_1_4,
                      ec1_dy1_dyi1_app1_apps1_e_e1.active_dw_ind_1 AS active_dw_ind_1_4,
                      ec1_dy1_dyi1_app1_apps1_e_e1.dw_last_update_date_time_1 AS dw_last_update_date_time_1_4
                    FROM
                      ec1_dy1_dyi1_app1_apps1_e_e1
                  UNION ALL
                  SELECT
                      e1.midas_encounter_id,
                      e1.eff_from_date,
                      e1.patient_dw_id,
                      e1.company_code,
                      e1.coid,
                      e1.midas_facility_code,
                      e1.eff_to_date,
                      e1.location_id,
                      e1.admitting_service_id,
                      e1.iq_adm_initial_review_empl_id,
                      e1.pat_acct_num,
                      e1.total_review_cnt,
                      e1.completed_review_cnt,
                      e1.midas_acct_num,
                      e1.initial_record_ind,
                      e1.admit_weekend_ind,
                      e1.pdu_ind,
                      e1.iq_adm_rev_type_ip_ind,
                      e1.iq_adm_rev_type_obs_ind,
                      e1.iq_adm_rev_type_ip_ptd_ind,
                      e1.iq_adm_rev_type_obs_ptd_ind,
                      e1.iq_adm_rev_type_ip_mn_met_ind,
                      e1.iq_adm_rev_type_obs_mn_met_ind,
                      e1.iq_adm_initial_review_hour,
                      e1.iq_adm_initial_rev_date_time,
                      e1.iq_adm_rev_criteria_status,
                      e1.iq_adm_rev_location_id,
                      e1.midas_encounter_last_updt_date,
                      e1.external_pa_referral_cm_ind,
                      e1.external_pa_referral_apel_ind,
                      e1.external_pa_referral_other_ind,
                      e1.external_pa_referral_cm_cnt,
                      e1.external_pa_referral_apel_cnt,
                      e1.external_pa_referral_other_cnt,
                      e1.external_pa_referral_disp_id,
                      e1.external_pa_referral_date_time,
                      e1.external_pa_referral_id,
                      e1.midas_last_ip_encounter_id,
                      e1.denial_onbase_unique_id,
                      e1.document_date,
                      e1.bpci_episode_group_id,
                      e1.bpci_data_science_percentage_num,
                      e1.bpci_data_science_date,
                      e1.concurrent_denial_code,
                      e1.inpatient_admit_review_cnt,
                      e1.first_inpatient_admit_review_date,
                      e1.last_inpatient_admit_review_date,
                      e1.inpatient_admit_review_delay_day_cnt,
                      e1.inpatient_admit_review_1_day_delay_ind,
                      e1.obs_prog_rev_after_disch_ind,
                      e1.go_live_date_ind,
                      e1.source_system_code,
                      e1.active_dw_ind,
                      e1.dw_last_update_date_time,
                      e1.hcm_avoid_denied_day_id,
                      e1.eff_from_date AS eff_from_date_1,
                      e1.patient_dw_id AS patient_dw_id_1,
                      e1.company_code AS company_code_1,
                      e1.coid AS coid_1,
                      e1.concurrent_review_id,
                      e1.update_by_user_id,
                      e1.reimbursement_group_code,
                      e1.reimbursement_group_name,
                      e1.reimbursement_group_start_date,
                      e1.midas_encounter_id AS midas_encounter_id_1,
                      e1.hcm_avoid_denied_day_ext_id,
                      e1.hcm_avoid_denied_last_update,
                      e1.tracer_comment,
                      e1.eff_to_date AS eff_to_date_1,
                      e1.initial_record_ind AS initial_record_ind_1,
                      e1.pdu_ind AS pdu_ind_1,
                      e1.source_system_code AS source_system_code_1,
                      e1.active_dw_ind AS active_dw_ind_1,
                      e1.dw_last_update_date_time AS dw_last_update_date_time_1,
                      e1.hcm_avoid_denied_day_info_id,
                      e1.hcm_avoid_denied_day_id AS hcm_avoid_denied_day_id_1,
                      e1.eff_from_date AS eff_from_date_1_0,
                      e1.entered_by_user_id,
                      e1.authorization_num,
                      e1.avoidable_denied_day_cnt,
                      e1.appeal_closed_date,
                      e1.avoid_denied_start_date,
                      e1.avoid_denied_end_date,
                      e1.date_of_denial,
                      e1.concurrent_denial_type_ind,
                      e1.day_recovered_cnt,
                      e1.dollar_avoidable_denied_amt,
                      e1.dollar_recovered_amt,
                      e1.insurance_num,
                      e1.location_id AS location_id_1,
                      e1.type_of_day_id,
                      e1.type_of_day_recovered_id,
                      e1.payor_avoid_denied_day_id,
                      e1.payor_avoid_denied_day_code,
                      e1.payor_avoid_denied_day_name,
                      e1.total_department_day_cnt,
                      e1.total_physician_day_cnt,
                      e1.eff_to_date AS eff_to_date_1_0,
                      e1.initial_record_ind AS initial_record_ind_1_0,
                      e1.pdu_ind AS pdu_ind_1_0,
                      e1.source_system_code AS source_system_code_1_0,
                      e1.active_dw_ind AS active_dw_ind_1_0,
                      e1.dw_last_update_date_time AS dw_last_update_date_time_1_0,
                      e1.hcm_avoid_denied_apel_id,
                      e1.hcm_avoid_denied_day_info_id AS hcm_avoid_denied_day_info_id_1,
                      e1.hcm_avoid_denied_day_id AS hcm_avoid_denied_day_id_1_0,
                      e1.eff_from_date AS eff_from_date_1_1,
                      e1.hcm_appeal_status_id,
                      e1.date_of_appeal,
                      e1.system_user_id,
                      e1.eff_to_date AS eff_to_date_1_1,
                      e1.initial_record_ind AS initial_record_ind_1_1,
                      e1.source_system_code AS source_system_code_1_1,
                      e1.active_dw_ind AS active_dw_ind_1_1,
                      e1.dw_last_update_date_time AS dw_last_update_date_time_1_1,
                      e1.hcm_appeal_status_id AS hcm_appeal_status_id_1,
                      e1.eff_from_date AS eff_from_date_1_2,
                      e1.hcm_appeal_status_code,
                      e1.hcm_appeal_status_name,
                      e1.eff_to_date AS eff_to_date_1_2,
                      e1.initial_record_ind AS initial_record_ind_1_2,
                      e1.source_system_code AS source_system_code_1_2,
                      e1.active_dw_ind AS active_dw_ind_1_2,
                      e1.dw_last_update_date_time AS dw_last_update_date_time_1_2,
                      e1.empl_id,
                      e1.eff_from_date AS eff_from_date_1_3,
                      e1.empl_num,
                      e1.empl_first_last_name,
                      e1.empl_first_name,
                      e1.empl_last_first_middle_name,
                      e1.empl_last_name,
                      e1.empl_middle_name,
                      e1.empl_first_middle_last_name,
                      e1.empl_name,
                      e1.birthdate,
                      e1.initial_hire_date,
                      e1.termination_date,
                      e1.sex_code,
                      e1.eff_to_date AS eff_to_date_1_3,
                      e1.initial_record_ind AS initial_record_ind_1_3,
                      e1.source_system_code AS source_system_code_1_3,
                      e1.active_dw_ind AS active_dw_ind_1_3,
                      e1.dw_last_update_date_time AS dw_last_update_date_time_1_3,
                      e1.null_0 AS empl_id_1,
                      e1.null_1 AS eff_from_date_1_4,
                      e1.null_2 AS empl_num_1,
                      e1.null_3 AS empl_first_last_name_1,
                      e1.null_4 AS empl_first_name_1,
                      e1.null_5 AS empl_last_first_middle_name_1,
                      e1.null_6 AS empl_last_name_1,
                      e1.null_7 AS empl_middle_name_1,
                      e1.null_8 AS empl_first_middle_last_name_1,
                      e1.null_9 AS empl_name_1,
                      e1.null_10 AS birthdate_1,
                      e1.null_11 AS initial_hire_date_1,
                      e1.null_12 AS termination_date_1,
                      e1.null_13 AS sex_code_1,
                      e1.null_14 AS eff_to_date_1_4,
                      e1.null_15 AS initial_record_ind_1_4,
                      e1.null_16 AS source_system_code_1_4,
                      e1.null_17 AS active_dw_ind_1_4,
                      e1.null_18 AS dw_last_update_date_time_1_4
                    FROM
                      e1
                ) AS ec1_dy1_dyi1_app1_apps1_e_e1_e1
              WHERE ec1_dy1_dyi1_app1_apps1_e_e1_e1.active_dw_ind = 'Y'
              QUALIFY row_number() OVER (PARTITION BY ec1_dy1_dyi1_app1_apps1_e_e1_e1.midas_encounter_id ORDER BY ec1_dy1_dyi1_app1_apps1_e_e1_e1.hcm_avoid_denied_apel_id DESC) = 1
          ) AS appl ON appl.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                crr.midas_encounter_id,
                crre.empl_name AS conc_reviewer,
                crre.empl_num AS conc_reviewer_id,
                rcc.interqual_review_code,
                iqm.iq_review_criteria_met_desc,
                pri.interqual_review_version_desc,
                siq.interqual_review_subset_desc,
                iq.primary_review_start_date_time
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS crr
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS rcc ON rcc.concurrent_review_id = crr.concurrent_review_id
                 AND upper(rcc.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS crre ON crre.empl_id = rcc.case_reviewed_by_employee_id
                 AND upper(crre.active_dw_ind) = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_interqual_review AS iq ON iq.interqual_review_code = rcc.interqual_review_code
                 AND upper(iq.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_cm_iq_criteria_met AS iqm ON iqm.iq_review_criteria_met_code = iq.iq_review_criteria_met_code
                 AND iqm.active_dw_ind = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_interqual_review_product AS pri ON upper(pri.interqual_review_product_code) = upper(iq.interqual_review_product_code)
                 AND upper(pri.interqual_review_version_code) = upper(iq.interqual_review_version_code)
                 AND pri.active_dw_ind = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_interqual_review_subset AS siq ON siq.interqual_review_product_code = iq.interqual_review_product_code
                 AND siq.interqual_review_subset_code = iq.interqual_review_subset_code
                 AND upper(siq.active_dw_ind) = 'Y'
              WHERE crr.active_dw_ind = 'Y'
              QUALIFY row_number() OVER (PARTITION BY crr.midas_encounter_id ORDER BY iq.primary_review_start_date_time DESC) = 1
          ) AS miq ON miq.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                crr4.midas_encounter_id,
                crre4.empl_name AS conc_reviewer,
                crre4.empl_num AS conc_reviewer_id,
                rcc4.review_date,
                hcmd.hcm_disposition_code,
                hcmd.hcm_disposition_desc,
                rcc4.conc_rev_conc_rev_id
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.hcm_concurrent_review AS crr4
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_conc_rev_conc_rev AS rcc4 ON rcc4.concurrent_review_id = crr4.concurrent_review_id
                 AND upper(rcc4.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.cm_employee AS crre4 ON crre4.empl_id = rcc4.case_reviewed_by_employee_id
                 AND upper(crre4.active_dw_ind) = 'Y'
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_disposition AS hcmd ON hcmd.disposition_id = rcc4.disposition_id
                 AND hcmd.active_dw_ind = 'Y'
              WHERE crr4.active_dw_ind = 'Y'
              QUALIFY row_number() OVER (PARTITION BY crr4.midas_encounter_id ORDER BY rcc4.conc_rev_conc_rev_id DESC) = 1
          ) AS miq4 ON miq4.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT
                fdi.patient_dw_id
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.full_documentation AS fdi
              WHERE upper(fdi.active_dw_ind) = 'Y'
               AND upper(fdi.revised_full_doc_ind) = 'Y'
               AND upper(fdi.avoidable_denied_days_ind) = 'N'
          ) AS fdf ON fdf.patient_dw_id = p.patient_dw_id
          LEFT OUTER JOIN (
            SELECT
                x.midas_encounter_id,
                max(CASE
                  WHEN x.xu_billing_condition_code_name = 'XF' THEN x.eff_from_date
                  ELSE CAST(NULL as DATE)
                END) AS cm_last_xf_code_applied_date,
                max(CASE
                  WHEN x.xu_billing_condition_code_name = 'XF' THEN 'Y'
                  ELSE 'N'
                END) AS cm_xf_ind,
                max(CASE
                  WHEN x.xu_billing_condition_code_name = 'XG' THEN x.eff_from_date
                  ELSE CAST(NULL as DATE)
                END) AS cm_last_xg_code_applied_date,
                max(CASE
                  WHEN x.xu_billing_condition_code_name = 'XG' THEN 'Y'
                  ELSE 'N'
                END) AS cm_xg_ind
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_billing_code_comment AS x
              WHERE x.active_dw_ind = 'Y'
               AND x.xu_billing_condition_code_name IN(
                'XF', 'XG'
              )
              GROUP BY 1
          ) AS xc ON xc.midas_encounter_id = e.midas_encounter_id
          LEFT OUTER JOIN (
            SELECT DISTINCT
                ed5.midas_encounter_id
              FROM
                `hca-hin-dev-cur-parallon`.edwcm_views.cm_encounter AS ed5
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day AS ds5 ON ds5.midas_encounter_id = ed5.midas_encounter_id
                 AND ds5.active_dw_ind = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.hcm_avoid_denied_day_info AS ia5 ON ia5.hcm_avoid_denied_day_id = ds5.hcm_avoid_denied_day_id
                 AND upper(ia5.active_dw_ind) = 'Y'
                INNER JOIN `hca-hin-dev-cur-parallon`.edwcm_views.ref_hcm_days_type AS ty ON ty.hcm_days_type_id = ia5.type_of_day_id
                 AND ty.active_dw_ind = 'Y'
              WHERE ed5.active_dw_ind = 'Y'
               AND upper(ty.hcm_days_type_name) LIKE '%DEN %'
          ) AS dend ON dend.midas_encounter_id = e.midas_encounter_id
        WHERE upper(ff.coid_status_code) = 'F'
         AND trim(cmf.midas_facility_code) IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY p.patient_dw_id ORDER BY e.dw_last_update_date_time DESC) = 1
    ) AS cmd ON cmd.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          fp_0.patient_dw_id,
          trim(pduu.determination_reason_desc) AS pdu_determination_reason_desc
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.prebill_denial_detail AS pduu
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp_0 ON fp_0.company_code = pduu.company_code
           AND fp_0.coid = pduu.coid
           AND fp_0.pat_acct_num = pduu.pat_acct_num
        WHERE upper(pduu.initial_auth_status_code) <> 'EXCEPTION'
         AND (trim(pduu.pilot_acct_ind) = 'N'
         OR trim(pduu.pilot_acct_ind) IS NULL)
        QUALIFY row_number() OVER (PARTITION BY pduu.coid, pduu.pat_acct_num ORDER BY pduu.rptg_date DESC) = 1
    ) AS pdu ON pdu.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          fn.patient_dw_id,
          fn.reporting_date,
          fn.pat_acct_num,
          fn.reporting_payor_dw_id,
          fn.reporting_iplan_id,
          fn.reporting_iplan_seq_num,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_medrec_request_date as STRING))), '1900-01-01')) as DATE) AS min_doc_req_medrec_request_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_medrec_request_date as STRING))), '1900-01-01')) as DATE) AS max_doc_req_medrec_request_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_medrec_sent_date as STRING))), '1900-01-01')) as DATE) AS min_doc_req_medrec_sent_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_medrec_sent_date as STRING))), '1900-01-01')) as DATE) AS max_doc_req_medrec_sent_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_ib_request_date as STRING))), '1900-01-01')) as DATE) AS min_doc_req_ib_request_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_ib_request_date as STRING))), '1900-01-01')) as DATE) AS max_doc_req_ib_request_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_ib_sent_date as STRING))), '1900-01-01')) as DATE) AS min_doc_req_ib_sent_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_ib_sent_date as STRING))), '1900-01-01')) as DATE) AS max_doc_req_ib_sent_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_request_date as STRING))), '1900-01-01')) as DATE) AS first_doc_request_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_request_date as STRING))), '1900-01-01')) as DATE) AS last_doc_request_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_sent_date as STRING))), '1900-01-01')) as DATE) AS first_doc_sent_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_sent_date as STRING))), '1900-01-01')) as DATE) AS last_doc_sent_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_received_date as STRING))), '1900-01-01')) as DATE) AS first_doc_received_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_received_date as STRING))), '1900-01-01')) as DATE) AS last_doc_received_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_approve_date as STRING))), '1900-01-01')) as DATE) AS first_doc_approved_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_approve_date as STRING))), '1900-01-01')) as DATE) AS last_doc_approved_date,
          CAST(trim(coalesce(min(trim(CAST(fn.doc_req_denied_date as STRING))), '1900-01-01')) as DATE) AS first_doc_denied_date,
          CAST(trim(coalesce(max(trim(CAST(fn.doc_req_denied_date as STRING))), '1900-01-01')) as DATE) AS last_doc_denied_date
        FROM
          (
            SELECT
                d.patient_dw_id,
                d.reporting_date,
                d.artiva_instance_code,
                d.coid,
                d.pat_acct_num,
                coalesce(p.total_billed_charges, CAST(0 as NUMERIC)) AS total_billed_charges_amt,
                ROUND(CASE
                  WHEN d.iplan_id = p.iplan_id_ins1 THEN p.payor_dw_id_ins1
                  WHEN d.iplan_id = p.iplan_id_ins2 THEN p.payor_dw_id_ins2
                  WHEN d.iplan_id = p.iplan_id_ins3 THEN p.payor_dw_id_ins3
                  WHEN i.major_payor_group_id = i1.major_payor_group_id THEN p.payor_dw_id_ins1
                  WHEN i.major_payor_group_id = i2.major_payor_group_id THEN p.payor_dw_id_ins2
                  WHEN i.major_payor_group_id = i3.major_payor_group_id THEN p.payor_dw_id_ins3
                  ELSE CAST(0 as NUMERIC)
                END, 0, 'ROUND_HALF_EVEN') AS reporting_payor_dw_id,
                CASE
                  WHEN d.iplan_id = p.iplan_id_ins1 THEN d.iplan_id
                  WHEN d.iplan_id = p.iplan_id_ins2 THEN d.iplan_id
                  WHEN d.iplan_id = p.iplan_id_ins3 THEN d.iplan_id
                  WHEN i.major_payor_group_id = i1.major_payor_group_id THEN p.iplan_id_ins1
                  WHEN i.major_payor_group_id = i2.major_payor_group_id THEN p.iplan_id_ins2
                  WHEN i.major_payor_group_id = i3.major_payor_group_id THEN p.iplan_id_ins3
                  ELSE 0
                END AS reporting_iplan_id,
                CASE
                  WHEN d.iplan_id = p.iplan_id_ins1 THEN 1
                  WHEN d.iplan_id = p.iplan_id_ins2 THEN 2
                  WHEN d.iplan_id = p.iplan_id_ins3 THEN 3
                  WHEN i.major_payor_group_id = i1.major_payor_group_id THEN 1
                  WHEN i.major_payor_group_id = i2.major_payor_group_id THEN 2
                  WHEN i.major_payor_group_id = i3.major_payor_group_id THEN 3
                  ELSE 0
                END AS reporting_iplan_seq_num,
                d.iplan_id AS doc_req_iplan_id,
                i.plan_desc AS doc_req_iplan_desc,
                d.status_code AS doc_req_status,
                d.create_date AS doc_req_submit_date,
                d.sent_date AS doc_req_sent_date,
                d.received_date AS doc_req_received_date,
                d.requested_date AS doc_req_request_date,
                d.approved_date AS doc_req_approve_date,
                d.denied_date AS doc_req_denied_date,
                d.med_rec_req_date AS doc_req_medrec_request_date,
                d.med_rec_sent_date AS doc_req_medrec_sent_date,
                d.itm_bill_req_date AS doc_req_ib_request_date,
                d.itm_bill_sent_date AS doc_req_ib_sent_date,
                d.first_letter_sent_date AS doc_req_first_letter_sent_date,
                d.second_letter_sent_date AS doc_req_second_letter_sent_date,
                d.third_letter_sent_date AS doc_req_third_letter_sent_date,
                CASE
                  WHEN coalesce(trim(d.cplt_med_rec_ind), 'N') = 'Y'
                   AND coalesce(trim(d.itm_bill_ind), 'N') = 'Y' THEN 'IB & MR Request'
                  WHEN coalesce(trim(d.cplt_med_rec_ind), 'N') = 'Y'
                   AND coalesce(trim(d.itm_bill_ind), 'N') <> 'Y' THEN 'Medical Record Request'
                  WHEN coalesce(trim(d.cplt_med_rec_ind), 'N') <> 'Y'
                   AND coalesce(trim(d.itm_bill_ind), 'N') = 'Y' THEN 'Itemized Bill Request'
                  ELSE 'All Other'
                END AS ib_medrec_doc_req_desc,
                CASE
                  WHEN coalesce(trim(d.cplt_med_rec_aprv_ind), 'N') = 'Y'
                   AND coalesce(trim(d.itm_bill_aprv_ind), 'N') = 'Y' THEN 'IB & MR Approved'
                  WHEN coalesce(trim(d.cplt_med_rec_aprv_ind), 'N') = 'Y'
                   AND coalesce(trim(d.itm_bill_aprv_ind), 'N') <> 'Y' THEN 'Medical Record Approved'
                  WHEN coalesce(trim(d.cplt_med_rec_aprv_ind), 'N') <> 'Y'
                   AND coalesce(trim(d.itm_bill_aprv_ind), 'N') = 'Y' THEN 'Itemized Bill Approved'
                  ELSE 'All Other'
                END AS ib_medrec_doc_req_apr_desc
              FROM
                `hca-hin-dev-cur-parallon`.edwpbs_views.collection_doc_req_dtl AS d
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_dimension AS fd ON fd.coid = d.coid
                 AND fd.company_code = d.company_code
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.facility_iplan AS i ON i.coid = d.coid
                 AND i.iplan_id = d.iplan_id
                 AND i.company_code = d.company_code
                INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS p ON p.patient_dw_id = d.patient_dw_id
                 AND p.company_code = d.company_code
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.facility_iplan AS i1 ON i1.payor_dw_id = p.payor_dw_id_ins1
                 AND i1.coid = p.coid
                 AND i1.company_code = p.company_code
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.facility_iplan AS i2 ON i2.payor_dw_id = p.payor_dw_id_ins2
                 AND i2.coid = p.coid
                 AND i2.company_code = p.company_code
                LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edw_pub_views.facility_iplan AS i3 ON i3.payor_dw_id = p.payor_dw_id_ins3
                 AND i3.coid = p.coid
                 AND i3.company_code = p.company_code
              WHERE d.company_code = 'H'
               AND fd.summary_7_member_ind = 'Y'
               AND coalesce(fd.osg_pas_ind, 'N') <> 'Y'
               AND coalesce(fd.summary_asd_member_ind, 'N') <> 'Y'
               AND coalesce(fd.summary_imaging_member_ind, 'N') <> 'Y'
               AND fd.pas_coid IN(
                '08591', '08648', '08942', '08945', '08947', '08948', '08949', '08950'
              )
               AND CAST(i.iplan_financial_class_code as INT64) IN(
                5, 7, 8, 9, 11, 12, 13, 14
              )
               AND coalesce(i.payer_type_code, '') NOT IN(
                'MV', 'VA'
              )
               AND coalesce(i.major_payor_group_id, format('%4d', 0)) NOT IN(
                '930'
              )
               AND coalesce(d.status_code, '') <> 'CANC  '
               AND d.create_date >= DATE '2020-01-01'
               AND d.reporting_date = (
                SELECT
                    max(dm.reporting_date) AS maxreporting_date
                  FROM
                    `hca-hin-dev-cur-parallon`.edwpbs_views.collection_doc_req_dtl AS dm
              )
          ) AS fn
        GROUP BY 1, 2, 3, 4, 5, 6
    ) AS prepay ON prepay.patient_dw_id = ccap.patient_dw_id
     AND prepay.reporting_payor_dw_id = ccap.payor_dw_id
     AND prepay.reporting_iplan_seq_num = ccap.iplan_order_num
    LEFT OUTER JOIN (
      SELECT
          pd.patient_dw_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpf_views.patient_diagnosis AS pd
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp_0 ON pd.company_code = fp_0.company_code
           AND fp_0.patient_dw_id = pd.patient_dw_id
        WHERE trim(pd.diag_code) = 'B9729'
         AND fp_0.discharge_date BETWEEN DATE '2020-01-27' AND DATE '2020-03-31'
         OR trim(pd.diag_code) = 'U071'
         AND fp_0.discharge_date >= DATE '2020-04-01'
        QUALIFY row_number() OVER (PARTITION BY pd.coid, pd.pat_acct_num ORDER BY pd.pa_last_update_date DESC) = 1
    ) AS cov ON cov.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          cri.patient_dw_id,
          cri.refund_iplan_id,
          cri.refund_amt,
          cri.entered_date,
          cri.refund_creation_user_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS cri
        WHERE cri.reporting_date = (
          SELECT
              max(crz.reporting_date)
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS crz
        )
         AND cri.refund_type_sid IN(
          2, 3
        )
        QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id, cri.refund_iplan_id ORDER BY cri.dw_last_update_date_time DESC) = 1
    ) AS cr1py ON cr1py.patient_dw_id = ccap.patient_dw_id
     AND cr1py.refund_iplan_id = ccap.iplan_id
    LEFT OUTER JOIN (
      SELECT
          cri.patient_dw_id,
          cri.refund_amt,
          cri.entered_date,
          cri.refund_creation_user_id
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS cri
        WHERE cri.reporting_date = (
          SELECT
              max(crz.reporting_date)
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS crz
        )
         AND cri.refund_type_sid = 4
        QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id ORDER BY cri.dw_last_update_date_time DESC) = 1
    ) AS cr1pt ON cr1pt.patient_dw_id = ccap.patient_dw_id
     AND ccap.iplan_order_num = 1
    LEFT OUTER JOIN (
      SELECT
          cri.patient_dw_id,
          CASE
            WHEN coalesce(cri.refund_iplan_id, NUMERIC '0') = 0 THEN cri.iplan_id_ins1
            ELSE cri.refund_iplan_id
          END AS iplan_id,
          cri.refund_type_sid,
          eiscs.credit_status_alias
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS cri
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_rcm_views.eis_credit_status_dim AS eiscs ON cri.credit_status_sid = eiscs.credit_status_sid
        WHERE cri.reporting_date = (
          SELECT
              max(crz.reporting_date)
            FROM
              `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_inventory AS crz
        )
         AND cri.credit_status_sid IS NOT NULL
         AND coalesce(cri.credit_status_sid, NUMERIC '0') <> 1
        QUALIFY row_number() OVER (PARTITION BY cri.patient_dw_id, iplan_id ORDER BY cri.dw_last_update_date_time DESC) = 1
    ) AS crb ON crb.patient_dw_id = ccap.patient_dw_id
     AND crb.iplan_id = ccap.iplan_id
    LEFT OUTER JOIN (
      SELECT
          crt.patient_dw_id,
          crt.discrepancy_source_desc,
          crt.reimbursement_impact_desc,
          crt.discrepancy_date_time,
          crt.request_date_time,
          crt.reprocess_reason_text,
          crt.status_desc
        FROM
          `hca-hin-dev-cur-parallon`.edwpbs_views.claim_reprocessing_tool_detail AS crt
        QUALIFY row_number() OVER (PARTITION BY crt.patient_dw_id ORDER BY crt.crt_log_id DESC) = 1
    ) AS msccr ON msccr.patient_dw_id = ccap.patient_dw_id
    LEFT OUTER JOIN (
      SELECT
          ccap_0.patient_dw_id,
          ccap_0.iplan_id,
          ccap_0.iplan_order_num,
          CASE
            WHEN ipbill.first_11x_bill_date = opbill.first_13x_bill_date
             AND ipbill.first_11x_bill_date IS NOT NULL THEN 'Y'
            WHEN maxopbill.max_13x_billed_charges <= fp_0.total_billed_charges * NUMERIC '0.8' THEN 'Y'
            ELSE 'N'
          END AS split_bill_ind
        FROM
          `hca-hin-dev-cur-parallon`.edwra_views.cc_account_payor AS ccap_0
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.fact_patient AS fp_0 ON fp_0.patient_dw_id = ccap_0.patient_dw_id
          INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_views.facility_iplan AS fi_0 ON fi_0.payor_dw_id = ccap_0.payor_dw_id
          LEFT OUTER JOIN (
            SELECT
                rh_837_claim.patient_dw_id,
                rh_837_claim.iplan_id,
                rh_837_claim.iplan_insurance_order_num,
                max(rh_837_claim.bill_date) AS last_11x_bill_date,
                min(rh_837_claim.bill_date) AS first_11x_bill_date
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim
              WHERE substr(rh_837_claim.bill_type_code, 1, 2) = '11'
               AND trim(rh_837_claim.bill_type_code) <> '118'
               AND upper(CASE
                WHEN upper(rh_837_claim.pas_coid) = '08910'
                 AND rh_837_claim.facility_prefix_code IN(
                  'D', 'H'
                ) THEN 'N'
                ELSE 'Y'
              END) = 'Y'
              GROUP BY 1, 2, 3
          ) AS ipbill ON ipbill.patient_dw_id = ccap_0.patient_dw_id
           AND ipbill.iplan_id = ccap_0.iplan_id
           AND ipbill.iplan_insurance_order_num = ccap_0.iplan_order_num
          LEFT OUTER JOIN (
            SELECT
                c.patient_dw_id,
                c.iplan_id,
                c.iplan_insurance_order_num,
                max(c.bill_date) AS last_13x_bill_date,
                min(c.bill_date) AS first_13x_bill_date
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim AS c
              WHERE substr(c.bill_type_code, 1, 2) = '13'
               AND upper(c.bill_type_code) <> '130'
               AND upper(CASE
                WHEN upper(c.pas_coid) = '08910'
                 AND c.facility_prefix_code IN(
                  'D', 'H'
                ) THEN 'N'
                ELSE 'Y'
              END) = 'Y'
              GROUP BY 1, 2, 3
          ) AS opbill ON opbill.patient_dw_id = ccap_0.patient_dw_id
           AND opbill.iplan_id = ccap_0.iplan_id
           AND opbill.iplan_insurance_order_num = ccap_0.iplan_order_num
          LEFT OUTER JOIN (
            SELECT
                c.patient_dw_id,
                c.iplan_id,
                c.iplan_insurance_order_num,
                c.bill_type_code,
                c.total_charge_amt AS max_13x_billed_charges
              FROM
                `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim AS c
                INNER JOIN (
                  SELECT
                      rh_837_claim.patient_dw_id,
                      rh_837_claim.iplan_id,
                      rh_837_claim.iplan_insurance_order_num,
                      max(rh_837_claim.bill_date) AS maxbill
                    FROM
                      `hca-hin-dev-cur-parallon`.edwpf_views.rh_837_claim
                    WHERE substr(rh_837_claim.bill_type_code, 1, 2) = '13'
                     AND upper(rh_837_claim.bill_type_code) <> '130'
                     AND upper(CASE
                      WHEN upper(rh_837_claim.pas_coid) = '08910'
                       AND rh_837_claim.facility_prefix_code IN(
                        'D', 'H'
                      ) THEN 'N'
                      ELSE 'Y'
                    END) = 'Y'
                    GROUP BY 1, 2, 3
                ) AS maxbill ON c.patient_dw_id = maxbill.patient_dw_id
                 AND c.iplan_id = maxbill.iplan_id
                 AND c.iplan_insurance_order_num = maxbill.iplan_insurance_order_num
                 AND c.bill_date = maxbill.maxbill
              WHERE substr(c.bill_type_code, 1, 2) = '13'
               AND upper(c.bill_type_code) <> '130'
               AND upper(CASE
                WHEN upper(c.pas_coid) = '08910'
                 AND c.facility_prefix_code IN(
                  'D', 'H'
                ) THEN 'N'
                ELSE 'Y'
              END) = 'Y'
               AND c.bill_date = maxbill.maxbill
              QUALIFY row_number() OVER (PARTITION BY c.patient_dw_id, c.iplan_id, c.iplan_insurance_order_num ORDER BY c.patient_dw_id) = 1
          ) AS maxopbill ON ccap_0.patient_dw_id = maxopbill.patient_dw_id
           AND ccap_0.iplan_id = maxopbill.iplan_id
           AND ccap_0.iplan_order_num = maxopbill.iplan_insurance_order_num
        WHERE ccap_0.iplan_order_num IN(
          1, 2, 3
        )
         AND ccap_0.dw_last_update_date_time = (
          SELECT
              max(ccap1.dw_last_update_date_time)
            FROM
              `hca-hin-dev-cur-parallon`.edwra_views.cc_account_payor AS ccap1
            WHERE ccap1.patient_dw_id = ccap_0.patient_dw_id
             AND ccap1.payor_dw_id = ccap_0.payor_dw_id
             AND ccap1.iplan_order_num = ccap_0.iplan_order_num
        )
    ) AS splitbill ON splitbill.patient_dw_id = ccap.patient_dw_id
     AND splitbill.iplan_id = ccap.iplan_id
     AND splitbill.iplan_order_num = ccap.iplan_order_num
    LEFT OUTER JOIN (
      SELECT
          aa.patient_dw_id,
          aa.payor_dw_id,
          aa.iplan_insurance_order_num,
          aa.activity_create_date_time AS last_scripted_applied_date_time,
          aa.create_login_userid,
          CASE
            WHEN scr_0.patient_dw_id IS NOT NULL THEN scra.metric_alias_name
            ELSE NULL
          END AS scripted_overpayment_desc
        FROM
          `hca-hin-dev-cur-parallon`.edwra_views.cc_account_activity AS aa
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.calculated_payor_overpayment AS scr_0 ON scr_0.patient_dw_id = aa.patient_dw_id
           AND scr_0.payor_dw_id = aa.payor_dw_id
           AND scr_0.rptg_date = td_sysfnlib.trunc(aa.activity_create_date_time)
          LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_views.dim_metric AS scra ON scra.metric_sid = coalesce(scr_0.overpayment_metric_sid, 10)
           AND upper(scra.metric_name_parent) = 'POTENTIAL_OVERPAYMENT_CATEGORY'
        WHERE aa.activity_type_id = 2
         AND upper(aa.activity_subject_text) LIKE '%TO%SCRIPTED%FROM%'
        QUALIFY row_number() OVER (PARTITION BY aa.patient_dw_id, aa.payor_dw_id, aa.iplan_insurance_order_num ORDER BY last_scripted_applied_date_time DESC) = 1
    ) AS scr ON scr.patient_dw_id = ccap.patient_dw_id
     AND scr.payor_dw_id = ccap.payor_dw_id
     AND scr.iplan_insurance_order_num = ccap.iplan_order_num
    LEFT OUTER JOIN (
      SELECT
          aa.patient_dw_id,
          aa.payor_dw_id,
          aa.iplan_insurance_order_num,
          aa.activity_create_date_time AS last_letter_sent_date_time
        FROM
          `hca-hin-dev-cur-parallon`.edwra_views.cc_account_activity AS aa
        WHERE trim(aa.activity_subject_text) = 'Potential Overpayment Letter Shipped - OEH'
         OR trim(aa.activity_subject_text) = 'Automated Potential Overpayment Letter Sent'
        QUALIFY row_number() OVER (PARTITION BY aa.patient_dw_id, aa.payor_dw_id, aa.iplan_insurance_order_num ORDER BY last_letter_sent_date_time DESC) = 1
    ) AS ltr ON ltr.patient_dw_id = ccap.patient_dw_id
     AND ltr.payor_dw_id = ccap.payor_dw_id
     AND ltr.iplan_insurance_order_num = ccap.iplan_order_num
  WHERE ccap.iplan_order_num IN(
    1, 2, 3
  )
   AND ccap.dw_last_update_date_time = (
    SELECT
        max(ccap1.dw_last_update_date_time)
      FROM
        `hca-hin-dev-cur-parallon`.edwra_views.cc_account_payor AS ccap1
      WHERE ccap1.patient_dw_id = ccap.patient_dw_id
       AND ccap1.iplan_order_num = ccap.iplan_order_num
  )
   AND cca.archive_state_ind = 'N'
;
