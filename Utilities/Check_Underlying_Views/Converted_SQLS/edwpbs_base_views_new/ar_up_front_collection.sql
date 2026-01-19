-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ar_up_front_collection.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ar_up_front_collection AS SELECT
    ar_up_front_collection.unit_num_sid,
    ar_up_front_collection.patient_type_sid,
    ar_up_front_collection.patient_financial_class_sid,
    ar_up_front_collection.payor_sid,
    ar_up_front_collection.account_status_sid,
    ar_up_front_collection.pat_acct_num,
    ar_up_front_collection.time_id,
    ar_up_front_collection.up_front_msr_sid,
    ar_up_front_collection.coid,
    ar_up_front_collection.pe_date,
    ar_up_front_collection.transaction_eff_date,
    ar_up_front_collection.company_code,
    ar_up_front_collection.patient_full_name,
    ar_up_front_collection.transaction_comment_text,
    ar_up_front_collection.transaction_amt,
    ar_up_front_collection.transaction_enter_date,
    ar_up_front_collection.patient_total_charge_amt,
    ar_up_front_collection.discharge_date,
    ar_up_front_collection.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ar_up_front_collection
;
