-- Translation time: 2024-02-16T20:48:08.013760Z
-- Translation job ID: 825ffe95-5d09-4d28-9bed-a2d58826a821
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwra_views/cc_remittance.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_parallon_ra_views_dataset_name }}.cc_remittance AS SELECT
    a.company_code,
    a.coid,
    a.patient_dw_id,
    a.payor_dw_id,
    a.remittance_advice_num,
    a.ra_log_date,
    a.log_id,
    a.log_sequence_num,
    a.remittance_header_id,
    a.remittance_id,
    a.unit_num,
    a.pat_acct_num,
    a.iplan_insurance_order_num,
    a.iplan_id,
    a.ra_ep_iplan_id,
    a.ar_bill_thru_date,
    a.ra_drg_code,
    a.ra_drg_weight,
    a.ra_hipps_code,
    a.ra_covered_days_num,
    a.ra_visit_cnt,
    a.ra_covered_days_visit_cnt,
    a.outlier_ind,
    a.ra_discharge_fraction_pct,
    a.ra_reimbursement_rate_pct,
    a.ra_non_replaced_blood_unit_qty,
    a.ra_prescription_qty,
    a.ra_total_charge_amt,
    a.ra_non_covered_charge_amt,
    a.ra_net_billed_charge_amt,
    a.ra_deductible_amt,
    a.ra_coinsurance_amt,
    a.ra_net_benefit_amt,
    a.ra_insurance_payment_amt,
    a.ra_patient_responsible_amt,
    a.ra_patient_qualifier_code,
    a.ra_claim_status_code,
    a.ra_patient_status_code,
    a.ra_claim_filing_ind_code,
    a.ra_medicare_format_code,
    a.ra_facility_type_code,
    a.ra_claim_frequency_code,
    a.ra_payer_claim_control_num,
    a.ra_receive_date,
    a.ra_service_start_date,
    a.ra_service_end_date,
    a.ra_coverage_expiration_date,
    a.ra_payer_patient_id,
    a.ra_payer_member_id,
    a.ra_replaced_by_remit_id,
    a.ra_source_type_id,
    a.active_ind,
    a.dw_last_update_date_time,
    a.source_system_code
  FROM
    {{ params.param_parallon_ra_base_views_dataset_name }}.cc_remittance AS a
    INNER JOIN {{ params.param_parallon_ra_base_views_dataset_name }}.secref_facility AS b ON rtrim(a.company_code) = rtrim(b.company_code)
     AND rtrim(a.coid) = rtrim(b.co_id)
     AND rtrim(b.user_id) = rtrim(session_user())
;
