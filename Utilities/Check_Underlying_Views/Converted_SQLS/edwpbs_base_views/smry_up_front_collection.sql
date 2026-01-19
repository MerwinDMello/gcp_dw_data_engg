-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/smry_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.smry_up_front_collection AS SELECT
    ar_up_front_collection.unit_num_sid,
    ar_up_front_collection.patient_type_sid,
    ar_up_front_collection.patient_financial_class_sid,
    ar_up_front_collection.payor_sid,
    ar_up_front_collection.account_status_sid,
    ar_up_front_collection.time_id,
    ar_up_front_collection.up_front_msr_sid,
    ar_up_front_collection.coid,
    ar_up_front_collection.pe_date,
    ar_up_front_collection.company_code,
    ROUND(ar_up_front_collection.transaction_amt, 3, 'ROUND_HALF_EVEN') AS transaction_amt,
    ROUND(ar_up_front_collection.patient_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS patient_total_charge_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ar_up_front_collection
;
