-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_account_detail_lvl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_detail_lvl AS SELECT
    pa.patient_dw_id,
    pa.company_code,
    pa.coid,
    pa.sub_unit_num,
    pa.pat_acct_num,
    pa.medical_record_num,
    pa.patient_type_code,
    pa.financial_class_code,
    pa.account_status_code,
    pa.admission_type_code,
    pa.admission_source_code,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE pa.patient_full_name
    END AS patient_full_name,
    CASE
      WHEN session_user() = so.userid THEN pa.social_security_num
      ELSE '***'
    END AS social_security_num,
    pa.birth_date,
    pa.gender_code,
    pa.marital_status_code,
    pa.patient_person_dw_id,
    pa.patient_address_dw_id,
    pa.admission_date,
    pa.discharge_date,
    pa.final_bill_date,
    pa.los_day_cnt,
    pa.drg_code,
    pa.principal_diag_code_icd9,
    pa.principal_diag_desc_icd9,
    pa.principal_diag_code_icd10,
    pa.principal_diag_desc_icd10,
    pa.service_code,
    pa.accomodation_code,
    pa.room_num,
    pa.bill_gen_date,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE pa.responsible_party_name
    END AS responsible_party_name,
    pa.pat_relationship_code,
    pa.agency_code,
    pa.initial_turnover_date,
    pa.stmt_issue_date,
    pa.patient_allowance_amt,
    pa.patient_adj_amt,
    pa.non_billable_adj_amt,
    pa.non_billable_charge_amt,
    pa.patient_balance_amt,
    pa.last_patient_pmt_amt,
    pa.last_patient_pmt_date,
    pa.pmt_arng_stmt_freq,
    pa.pmt_arng_start_date,
    pa.previous_due_amt,
    pa.current_due_amt,
    pa.patient_overdue_amt,
    pa.patient_min_due_amt,
    pa.total_patient_pmt_amt,
    pa.total_charge_amt,
    pa.total_account_balance_amt,
    pa.total_pmt_amt,
    pa.total_allowance_amt,
    pa.total_write_off_amt,
    pa.total_adj_amt,
    pa.stmt_cnt,
    pa.last_pmt_rcvd_date,
    pa.recall_date,
    pa.initial_bad_debt_prelist_date,
    pa.bad_debt_reason_code,
    pa.patient_turnover_date,
    pa.agency_type_code,
    pa.current_collection_series_num,
    pa.previous_collection_series_num,
    pa.pa_restore_date,
    pa_addr.address_1_text AS patient_address_1_text,
    pa_addr.address_2_text AS patient_address_2_text,
    pa_addr.city_state_name AS patient_city_state_name,
    pa_addr.state_code AS patient_state_code,
    pa_addr.zip_code AS patient_zip_code,
    pa_addr.county_code AS patient_county_code,
    pa_addr.country_name AS patient_country_name,
    pa_addr.email_address_text AS patient_email_address_text,
    pi_1.payor_dw_id AS ins1_payor_dw_id,
    pi_2.payor_dw_id AS ins2_payor_dw_id,
    pi_3.payor_dw_id AS ins3_payor_dw_id,
    pi_1.group_name AS ins1_group_name,
    pi_2.group_name AS ins2_group_name,
    pi_3.group_name AS ins3_group_name,
    pi_1.group_num AS ins1_group_num,
    pi_2.group_num AS ins2_group_num,
    pi_3.group_num AS ins3_group_num,
    pi_1.iplan_id AS ins1_iplan_id,
    pi_2.iplan_id AS ins2_iplan_id,
    pi_3.iplan_id AS ins3_iplan_id,
    pi_1.financial_class_code AS ins1_financial_class_code,
    pi_2.financial_class_code AS ins2_financial_class_code,
    pi_3.financial_class_code AS ins3_financial_class_code,
    pi_1.hic_claim_num AS ins1_hic_claim_num,
    pi_2.hic_claim_num AS ins2_hic_claim_num,
    pi_3.hic_claim_num AS ins3_hic_claim_num,
    pi_1.claim_submit_date AS ins1_claim_submit_date,
    pi_2.claim_submit_date AS ins2_claim_submit_date,
    pi_3.claim_submit_date AS ins3_claim_submit_date,
    pi_1.denial_code AS ins1_denial_code,
    pi_2.denial_code AS ins2_denial_code,
    pi_3.denial_code AS ins3_denial_code,
    pi_1.denial_status_code AS ins1_denial_status_code,
    pi_2.denial_status_code AS ins2_denial_status_code,
    pi_3.denial_status_code AS ins3_denial_status_code,
    pi_1.mail_to_name AS ins1_mail_to_name,
    pi_2.mail_to_name AS ins2_mail_to_name,
    pi_3.mail_to_name AS ins3_mail_to_name,
    pi_1.address_dw_id AS ins1_address_dw_id,
    pi_2.address_dw_id AS ins2_address_dw_id,
    pi_3.address_dw_id AS ins3_address_dw_id,
    pi_1.log_id AS ins1_log_id,
    pi_2.log_id AS ins2_log_id,
    pi_3.log_id AS ins3_log_id,
    pi_1.allowance_amt AS ins1_allowance_amt,
    pi_2.allowance_amt AS ins2_allowance_amt,
    pi_3.allowance_amt AS ins3_allowance_amt,
    pi_1.adj_amt AS ins1_adj_amt,
    pi_2.adj_amt AS ins2_adj_amt,
    pi_3.adj_amt AS ins3_adj_amt,
    pi_1.payor_balance_amt AS ins1_payor_balance_amt,
    pi_2.payor_balance_amt AS ins2_payor_balance_amt,
    pi_3.payor_balance_amt AS ins3_payor_balance_amt,
    pi_1.last_pmt_rcvd_amt AS ins1_last_pmt_rcvd_amt,
    pi_2.last_pmt_rcvd_amt AS ins2_last_pmt_rcvd_amt,
    pi_3.last_pmt_rcvd_amt AS ins3_last_pmt_rcvd_amt,
    pi_1.last_pmt_rcvd_date AS ins1_last_pmt_rcvd_date,
    pi_2.last_pmt_rcvd_date AS ins2_last_pmt_rcvd_date,
    pi_3.last_pmt_rcvd_date AS ins3_last_pmt_rcvd_date,
    pi_1.total_pmt_rcvd_amt AS ins1_total_pmt_rcvd_amt,
    pi_2.total_pmt_rcvd_amt AS ins2_total_pmt_rcvd_amt,
    pi_3.total_pmt_rcvd_amt AS ins3_total_pmt_rcvd_amt,
    pi_1.gross_reimbursement_amt AS ins1_gross_reimbursement_amt,
    pi_2.gross_reimbursement_amt AS ins2_gross_reimbursement_amt,
    pi_3.gross_reimbursement_amt AS ins3_gross_reimbursement_amt,
    pi_1.gross_reimbursement_var_amt AS ins1_gross_reimbursement_var_amt,
    pi_2.gross_reimbursement_var_amt AS ins2_gross_reimbursement_var_amt,
    pi_3.gross_reimbursement_var_amt AS ins3_gross_reimbursement_var_amt,
    pi_1.payor_liability_amt AS ins1_payor_liability_amt,
    pi_2.payor_liability_amt AS ins2_payor_liability_amt,
    pi_3.payor_liability_amt AS ins3_payor_liability_amt,
    ins1_addr.address_1_text AS ins1_address_1_text,
    ins1_addr.address_2_text AS ins1_address_2_text,
    ins1_addr.city_state_name AS ins1_city_state_name,
    ins1_addr.zip_code AS ins1_zip_code,
    ins1_addr.county_code AS ins1_county_code,
    ins1_addr.country_name AS ins1_country_name,
    ins1_addr.email_address_text AS ins1_email_address_text,
    ins2_addr.address_1_text AS ins2_address_1_text,
    ins2_addr.address_2_text AS ins2_address_2_text,
    ins2_addr.city_state_name AS ins2_city_state_name,
    ins2_addr.zip_code AS ins2_zip_code,
    ins2_addr.county_code AS ins2_county_code,
    ins2_addr.country_name AS ins2_country_name,
    ins2_addr.email_address_text AS ins2_email_address_text,
    ins3_addr.address_1_text AS ins3_address_1_text,
    ins3_addr.address_2_text AS ins3_address_2_text,
    ins3_addr.city_state_name AS ins3_city_state_name,
    ins3_addr.zip_code AS ins3_zip_code,
    ins3_addr.county_code AS ins3_county_code,
    ins3_addr.country_name AS ins3_country_name,
    ins3_addr.email_address_text AS ins3_email_address_text,
    pa.source_system_code,
    pa.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_detail AS pa
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON pa.company_code = b.company_code
     AND pa.coid = b.co_id
     AND b.user_id = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND security_mask_and_exception.masked_column_code = 'PN'
    ) AS pn ON pn.userid = session_user()
    LEFT OUTER JOIN (
      SELECT
          security_mask_and_exception.userid,
          security_mask_and_exception.masked_column_code
        FROM
          `hca-hin-dev-cur-parallon`.edw_sec_base_views.security_mask_and_exception
        WHERE security_mask_and_exception.userid = session_user()
         AND security_mask_and_exception.masked_column_code = 'SSN'
    ) AS so ON so.userid = session_user()
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.address AS pa_addr ON pa_addr.address_dw_id = pa.patient_address_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_insurance AS pi_1 ON pa.patient_dw_id = pi_1.patient_dw_id
     AND pi_1.insurance_order_num = 1
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_insurance AS pi_2 ON pa.patient_dw_id = pi_2.patient_dw_id
     AND pi_2.insurance_order_num = 2
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_insurance AS pi_3 ON pa.patient_dw_id = pi_3.patient_dw_id
     AND pi_3.insurance_order_num = 3
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.address AS ins1_addr ON ins1_addr.address_dw_id = pi_1.address_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.address AS ins2_addr ON ins2_addr.address_dw_id = pi_2.address_dw_id
    LEFT OUTER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.address AS ins3_addr ON ins3_addr.address_dw_id = pi_3.address_dw_id
;
