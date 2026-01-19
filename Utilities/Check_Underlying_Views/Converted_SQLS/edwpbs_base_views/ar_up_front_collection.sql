-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_up_front_collection AS SELECT
    ar_up_front_collection.unit_num_sid,
    ar_up_front_collection.patient_type_sid,
    ar_up_front_collection.patient_financial_class_sid,
    ar_up_front_collection.payor_sid,
    ar_up_front_collection.account_status_sid,
    ROUND(ar_up_front_collection.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    ar_up_front_collection.time_id,
    ar_up_front_collection.up_front_msr_sid,
    ar_up_front_collection.coid,
    ar_up_front_collection.pe_date,
    ar_up_front_collection.transaction_eff_date,
    ar_up_front_collection.company_code,
    ar_up_front_collection.patient_full_name,
    ar_up_front_collection.transaction_comment_text,
    ROUND(ar_up_front_collection.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
    ar_up_front_collection.transaction_enter_date,
    ROUND(ar_up_front_collection.patient_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS patient_total_charge_amt,
    ar_up_front_collection.discharge_date,
    ar_up_front_collection.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ar_up_front_collection
;
