-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_credit_balance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_credit_balance AS SELECT
    a.patient_sid,
    a.unit_num_sid,
    a.account_status_sid,
    a.patient_financial_class_sid,
    a.patient_type_sid,
    a.payor_sid,
    a.payor_sequence_sid,
    a.credit_status_sid,
    a.cr_bal_orig_age_month_sid,
    a.date_sid,
    a.patient_dw_id,
    a.company_code,
    a.coid,
    a.credit_balance_amt,
    a.credit_balance_count,
    a.discharge_date,
    a.credit_balance_date,
    a.created_date,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_credit_balance AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
