-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/patient_account_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_balance AS SELECT
    patient_account_balance.patient_dw_id,
    patient_account_balance.eff_from_date,
    patient_account_balance.coid,
    patient_account_balance.company_code,
    patient_account_balance.pat_acct_num,
    patient_account_balance.account_status_code,
    patient_account_balance.payor_dw_id_ins1,
    patient_account_balance.payor_dw_id_ins2,
    patient_account_balance.payor_dw_id_ins3,
    patient_account_balance.denial_code_ins1,
    patient_account_balance.denial_code_ins2,
    patient_account_balance.denial_code_ins3,
    patient_account_balance.denial_status_code_ins1,
    patient_account_balance.denial_status_code_ins2,
    patient_account_balance.denial_status_code_ins3,
    patient_account_balance.payor_balance_amt_ins1,
    patient_account_balance.payor_balance_amt_ins2,
    patient_account_balance.payor_balance_amt_ins3,
    patient_account_balance.patient_balance_amt,
    patient_account_balance.total_account_balance_amt,
    patient_account_balance.eff_to_date,
    patient_account_balance.active_ind,
    patient_account_balance.source_system_code,
    patient_account_balance.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.patient_account_balance
;
