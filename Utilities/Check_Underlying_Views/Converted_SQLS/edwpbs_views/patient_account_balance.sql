-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_account_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_balance
   OPTIONS(description='Change data capture patient and payor AR balance amounts  from PA daily.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.eff_from_date,
      a.coid,
      a.company_code,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.account_status_code,
      ROUND(a.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      ROUND(a.payor_dw_id_ins2, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins2,
      ROUND(a.payor_dw_id_ins3, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins3,
      a.denial_code_ins1,
      a.denial_code_ins2,
      a.denial_code_ins3,
      a.denial_status_code_ins1,
      a.denial_status_code_ins2,
      a.denial_status_code_ins3,
      ROUND(a.payor_balance_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins1,
      ROUND(a.payor_balance_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins2,
      ROUND(a.payor_balance_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins3,
      ROUND(a.patient_balance_amt, 3, 'ROUND_HALF_EVEN') AS patient_balance_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      a.eff_to_date,
      a.active_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_balance AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
