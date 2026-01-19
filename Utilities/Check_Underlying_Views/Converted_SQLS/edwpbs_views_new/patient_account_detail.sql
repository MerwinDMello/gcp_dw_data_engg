-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_account_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_detail AS SELECT
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.sub_unit_num,
    a.pat_acct_num,
    a.medical_record_num,
    a.patient_type_code,
    a.financial_class_code,
    a.account_status_code,
    a.admission_type_code,
    a.admission_source_code,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.patient_full_name
    END AS patient_full_name,
    CASE
      WHEN session_user() = so.userid THEN a.social_security_num
      ELSE '***'
    END AS social_security_num,
    a.birth_date,
    a.gender_code,
    a.marital_status_code,
    a.patient_person_dw_id,
    a.patient_address_dw_id,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.los_day_cnt,
    a.drg_code,
    a.principal_diag_code_icd9,
    a.principal_diag_desc_icd9,
    a.principal_diag_code_icd10,
    a.principal_diag_desc_icd10,
    a.service_code,
    a.accomodation_code,
    a.room_num,
    a.bill_gen_date,
    CASE
      WHEN session_user() = pn.userid THEN '***'
      ELSE a.responsible_party_name
    END AS responsible_party_name,
    a.pat_relationship_code,
    a.agency_code,
    a.initial_turnover_date,
    a.stmt_issue_date,
    a.patient_allowance_amt,
    a.patient_adj_amt,
    a.non_billable_adj_amt,
    a.non_billable_charge_amt,
    a.patient_balance_amt,
    a.last_patient_pmt_amt,
    a.last_patient_pmt_date,
    a.pmt_arng_stmt_freq,
    a.pmt_arng_start_date,
    a.previous_due_amt,
    a.current_due_amt,
    a.patient_overdue_amt,
    a.patient_min_due_amt,
    a.total_patient_pmt_amt,
    a.total_charge_amt,
    a.total_account_balance_amt,
    a.total_pmt_amt,
    a.total_allowance_amt,
    a.total_write_off_amt,
    a.total_adj_amt,
    a.stmt_cnt,
    a.last_pmt_rcvd_date,
    a.recall_date,
    a.initial_bad_debt_prelist_date,
    a.bad_debt_reason_code,
    a.patient_turnover_date,
    a.agency_type_code,
    a.current_collection_series_num,
    a.previous_collection_series_num,
    a.pa_restore_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
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
;
