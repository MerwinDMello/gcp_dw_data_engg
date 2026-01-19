-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_cr_balance_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_cr_balance_nkey AS SELECT
    a.patient_sid,
    a.date_sid,
    a.pe_date,
    a.company_code,
    a.coid,
    a.unit_num_sid,
    a.unit_num_member,
    a.unit_num_alias,
    a.account_status_sid,
    a.account_status_member,
    a.account_status_alias,
    a.patient_type_sid,
    a.patient_type_member,
    a.patient_type_alias,
    a.patient_financial_class_sid,
    a.patient_financial_class_member,
    a.patient_financial_class_alias,
    a.payor_sid,
    a.payor_member,
    a.payor_alias,
    a.payor_sequence_sid,
    a.payor_sequence_member,
    a.payor_sequence_alias,
    a.cr_bal_orig_age_month_sid,
    a.cr_bal_orig_age_member,
    a.cr_bal_orig_age_alias,
    a.credit_status_sid,
    a.credit_status_member,
    a.credit_status_alias,
    a.same_store_sid,
    a.same_store_name_child,
    a.same_store_alias_name,
    a.credit_balance_amt,
    a.credit_balance_count,
    a.discharge_date,
    a.credit_balance_date,
    a.created_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_cr_balance_nkey AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
