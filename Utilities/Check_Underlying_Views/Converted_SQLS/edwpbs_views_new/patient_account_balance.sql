-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/patient_account_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.patient_account_balance AS SELECT
    a.patient_dw_id,
    a.eff_from_date,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.account_status_code,
    a.payor_dw_id_ins1,
    a.payor_dw_id_ins2,
    a.payor_dw_id_ins3,
    a.denial_code_ins1,
    a.denial_code_ins2,
    a.denial_code_ins3,
    a.denial_status_code_ins1,
    a.denial_status_code_ins2,
    a.denial_status_code_ins3,
    a.payor_balance_amt_ins1,
    a.payor_balance_amt_ins2,
    a.payor_balance_amt_ins3,
    a.patient_balance_amt,
    a.total_account_balance_amt,
    a.eff_to_date,
    a.active_ind,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.patient_account_balance AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
