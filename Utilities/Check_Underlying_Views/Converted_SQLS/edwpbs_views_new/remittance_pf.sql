-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_pf AS SELECT
    bv.company_code,
    bv.coid,
    bv.patient_dw_id,
    bv.payor_dw_id,
    bv.remittance_advice_num,
    bv.ra_log_date,
    bv.log_id,
    bv.log_sequence_num,
    bv.pat_acct_num,
    bv.iplan_id,
    bv.cc_ep_iplan_id,
    bv.ar_bill_thru_date,
    bv.payment_date,
    bv.ra_reference_num,
    bv.cc_remittance_header_id,
    bv.cc_remittance_id,
    bv.cc_replaced_by_remittance_id,
    bv.ra_transaction_date,
    bv.ra_drg_code,
    bv.ra_hipps_code,
    bv.ra_covered_days_num,
    bv.ra_total_charge_amt,
    bv.ra_non_covered_charge_amt,
    bv.ra_net_billed_charge_amt,
    bv.ra_deductible_amt,
    bv.ra_coinsurance_amt,
    bv.ra_net_benefit_amt,
    bv.ra_insurance_payment_amt,
    bv.ra_reversal_code,
    bv.outlier_ind,
    bv.cc_visit_cnt,
    bv.cc_covered_days_visit_cnt,
    bv.cc_discharge_fraction_pct,
    bv.cc_reimbursement_rate_pct,
    bv.cc_non_replaced_blood_unit_qty,
    bv.cc_prescription_qty,
    bv.cc_patient_responsible_amt,
    bv.cc_patient_qualifier_code,
    bv.cc_claim_status_code,
    bv.cc_patient_status_code,
    bv.cc_claim_filing_ind_code,
    bv.cc_medicare_format_code,
    bv.cc_claim_frequency_code,
    bv.cc_payer_claim_control_num,
    bv.cc_recieve_date,
    bv.cc_service_start_date,
    bv.cc_service_end_date,
    bv.cc_coverage_expiration_date,
    bv.cc_payer_patient_id,
    bv.cc_payer_member_id,
    bv.cc_source_type_id,
    bv.active_ind,
    bv.dw_last_update_date_time,
    bv.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_pf AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON bv.coid = sf.co_id
     AND sf.user_id = session_user()
;
