-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_account_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_balance
   OPTIONS(description='Change data capture patient and payor AR balance amounts  from PA daily.')
  AS SELECT
      ROUND(patient_account_balance.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      patient_account_balance.eff_from_date,
      patient_account_balance.coid,
      patient_account_balance.company_code,
      ROUND(patient_account_balance.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      patient_account_balance.account_status_code,
      ROUND(patient_account_balance.payor_dw_id_ins1, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins1,
      ROUND(patient_account_balance.payor_dw_id_ins2, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins2,
      ROUND(patient_account_balance.payor_dw_id_ins3, 0, 'ROUND_HALF_EVEN') AS payor_dw_id_ins3,
      patient_account_balance.denial_code_ins1,
      patient_account_balance.denial_code_ins2,
      patient_account_balance.denial_code_ins3,
      patient_account_balance.denial_status_code_ins1,
      patient_account_balance.denial_status_code_ins2,
      patient_account_balance.denial_status_code_ins3,
      ROUND(patient_account_balance.payor_balance_amt_ins1, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins1,
      ROUND(patient_account_balance.payor_balance_amt_ins2, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins2,
      ROUND(patient_account_balance.payor_balance_amt_ins3, 3, 'ROUND_HALF_EVEN') AS payor_balance_amt_ins3,
      ROUND(patient_account_balance.patient_balance_amt, 3, 'ROUND_HALF_EVEN') AS patient_balance_amt,
      ROUND(patient_account_balance.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      patient_account_balance.eff_to_date,
      patient_account_balance.active_ind,
      patient_account_balance.source_system_code,
      patient_account_balance.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.patient_account_balance
  ;
