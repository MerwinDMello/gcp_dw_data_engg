-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/remittance_pf.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.remittance_pf AS SELECT
    bv.company_code,
    bv.coid,
    ROUND(bv.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ROUND(bv.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
    bv.remittance_advice_num,
    bv.ra_log_date,
    bv.log_id,
    bv.log_sequence_num,
    ROUND(bv.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    bv.iplan_id,
    bv.cc_ep_iplan_id,
    bv.ar_bill_thru_date,
    bv.payment_date,
    bv.ra_reference_num,
    ROUND(bv.cc_remittance_header_id, 0, 'ROUND_HALF_EVEN') AS cc_remittance_header_id,
    ROUND(bv.cc_remittance_id, 0, 'ROUND_HALF_EVEN') AS cc_remittance_id,
    ROUND(bv.cc_replaced_by_remittance_id, 0, 'ROUND_HALF_EVEN') AS cc_replaced_by_remittance_id,
    bv.ra_transaction_date,
    bv.ra_drg_code,
    bv.ra_hipps_code,
    bv.ra_covered_days_num,
    ROUND(bv.ra_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS ra_total_charge_amt,
    ROUND(bv.ra_non_covered_charge_amt, 3, 'ROUND_HALF_EVEN') AS ra_non_covered_charge_amt,
    ROUND(bv.ra_net_billed_charge_amt, 3, 'ROUND_HALF_EVEN') AS ra_net_billed_charge_amt,
    ROUND(bv.ra_deductible_amt, 3, 'ROUND_HALF_EVEN') AS ra_deductible_amt,
    ROUND(bv.ra_coinsurance_amt, 3, 'ROUND_HALF_EVEN') AS ra_coinsurance_amt,
    ROUND(bv.ra_net_benefit_amt, 3, 'ROUND_HALF_EVEN') AS ra_net_benefit_amt,
    ROUND(bv.ra_insurance_payment_amt, 3, 'ROUND_HALF_EVEN') AS ra_insurance_payment_amt,
    bv.ra_reversal_code,
    bv.outlier_ind,
    bv.cc_visit_cnt,
    bv.cc_covered_days_visit_cnt,
    ROUND(bv.cc_discharge_fraction_pct, 4, 'ROUND_HALF_EVEN') AS cc_discharge_fraction_pct,
    ROUND(bv.cc_reimbursement_rate_pct, 4, 'ROUND_HALF_EVEN') AS cc_reimbursement_rate_pct,
    ROUND(bv.cc_non_replaced_blood_unit_qty, 2, 'ROUND_HALF_EVEN') AS cc_non_replaced_blood_unit_qty,
    ROUND(bv.cc_prescription_qty, 2, 'ROUND_HALF_EVEN') AS cc_prescription_qty,
    ROUND(bv.cc_patient_responsible_amt, 3, 'ROUND_HALF_EVEN') AS cc_patient_responsible_amt,
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
    ROUND(bv.cc_source_type_id, 0, 'ROUND_HALF_EVEN') AS cc_source_type_id,
    bv.active_ind,
    bv.dw_last_update_date_time,
    bv.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.remittance_pf AS bv
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS sf ON upper(bv.coid) = upper(sf.co_id)
     AND sf.user_id = session_user()
;
