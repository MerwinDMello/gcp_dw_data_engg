-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_patient_iplan_conv.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_patient_iplan_conv AS SELECT
    fact_patient_iplan_conv.patient_dw_id,
    fact_patient_iplan_conv.month_id,
    fact_patient_iplan_conv.pe_date,
    fact_patient_iplan_conv.week_month_code,
    fact_patient_iplan_conv.coid,
    fact_patient_iplan_conv.company_code,
    fact_patient_iplan_conv.unit_num,
    fact_patient_iplan_conv.pat_acct_num,
    fact_patient_iplan_conv.same_store_sid,
    fact_patient_iplan_conv.from_patient_type_code,
    fact_patient_iplan_conv.to_patient_type_code,
    fact_patient_iplan_conv.from_financial_class_code,
    fact_patient_iplan_conv.to_financial_class_code,
    fact_patient_iplan_conv.from_iplan_id,
    fact_patient_iplan_conv.to_iplan_id,
    fact_patient_iplan_conv.from_case_cnt,
    fact_patient_iplan_conv.to_case_cnt,
    fact_patient_iplan_conv.from_calc_amt,
    fact_patient_iplan_conv.to_calc_amt,
    fact_patient_iplan_conv.conv_change_amt,
    fact_patient_iplan_conv.from_total_billed_charge_amt,
    fact_patient_iplan_conv.to_total_billed_charge_amt,
    fact_patient_iplan_conv.from_eor_gross_reimbursement_amt,
    fact_patient_iplan_conv.to_eor_gross_reimbursement_amt,
    fact_patient_iplan_conv.from_financial_class_payment_rate_calc,
    fact_patient_iplan_conv.to_financial_class_payment_rate_calc,
    fact_patient_iplan_conv.from_iplan_payment_rate_calc,
    fact_patient_iplan_conv.to_iplan_payment_rate_calc,
    fact_patient_iplan_conv.from_sma_payment_rate_calc,
    fact_patient_iplan_conv.to_sma_payment_rate_calc,
    fact_patient_iplan_conv.source_system_code,
    fact_patient_iplan_conv.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_patient_iplan_conv
;
