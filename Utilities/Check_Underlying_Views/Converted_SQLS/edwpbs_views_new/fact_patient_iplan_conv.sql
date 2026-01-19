-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_patient_iplan_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_patient_iplan_conv AS SELECT
    a.patient_dw_id,
    a.month_id,
    a.pe_date,
    a.week_month_code,
    a.coid,
    a.company_code,
    a.unit_num,
    a.pat_acct_num,
    a.same_store_sid,
    a.from_patient_type_code,
    a.to_patient_type_code,
    a.from_financial_class_code,
    a.to_financial_class_code,
    a.from_iplan_id,
    a.to_iplan_id,
    a.from_case_cnt,
    a.to_case_cnt,
    a.from_calc_amt,
    a.to_calc_amt,
    a.conv_change_amt,
    a.from_total_billed_charge_amt,
    a.to_total_billed_charge_amt,
    a.from_eor_gross_reimbursement_amt,
    a.to_eor_gross_reimbursement_amt,
    a.from_financial_class_payment_rate_calc,
    a.to_financial_class_payment_rate_calc,
    a.from_iplan_payment_rate_calc,
    a.to_iplan_payment_rate_calc,
    a.from_sma_payment_rate_calc,
    a.to_sma_payment_rate_calc,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_iplan_conv AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
