-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/him_unbilled_daily.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.him_unbilled_daily AS SELECT
    a.unit_num,
    a.effective_date,
    ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    a.company_code,
    a.coid,
    a.patient_name,
    a.pa_pat_acct_type,
    a.him_pat_acct_type,
    a.medical_record_num,
    a.admission_date,
    a.discharge_date,
    a.final_bill_date,
    a.days_since_discharge,
    a.iplan_id_ins1,
    ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
    ROUND(a.gross_charge_amt, 3, 'ROUND_HALF_EVEN') AS gross_charge_amt,
    ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    a.account_process_ind,
    a.unbilled_responsibility_ind,
    a.him_responsibility_ind,
    a.him_location,
    a.service_code_mnemonic,
    a.patient_discharge_month_age,
    a.unbilled_reason_code,
    a.responsible_physician,
    a.responsible_physician_mnemonic,
    a.attending_physician,
    a.attending_physician_mnemonic
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.him_unbilled_daily AS a
;
