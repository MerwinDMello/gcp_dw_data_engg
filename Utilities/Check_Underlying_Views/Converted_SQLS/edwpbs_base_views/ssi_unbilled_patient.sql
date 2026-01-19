-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ssi_unbilled_patient.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ssi_unbilled_patient AS SELECT
    ROUND(ssi_unbilled_patient.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
    ssi_unbilled_patient.effective_date,
    ssi_unbilled_patient.coid,
    ssi_unbilled_patient.company_code,
    ROUND(ssi_unbilled_patient.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
    ssi_unbilled_patient.unit_num,
    ssi_unbilled_patient.claim_id,
    ssi_unbilled_patient.ssi_insurance_id,
    ssi_unbilled_patient.medical_record_num,
    ssi_unbilled_patient.service_start_date,
    ssi_unbilled_patient.service_end_date,
    ssi_unbilled_patient.ssi_unbilled_reason_code,
    ssi_unbilled_patient.ssi_queue_dept_id,
    ssi_unbilled_patient.ssi_system_id,
    ssi_unbilled_patient.ssi_unbilled_status_code,
    ssi_unbilled_patient.claim_date,
    ssi_unbilled_patient.ime_claim_ind,
    ssi_unbilled_patient.type_of_bill_code,
    ssi_unbilled_patient.bill_form,
    ROUND(ssi_unbilled_patient.ssi_total_charge_amt, 3, 'ROUND_HALF_EVEN') AS ssi_total_charge_amt,
    ssi_unbilled_patient.ssi_request_type_id,
    ssi_unbilled_patient.ssi_payor_sub_id,
    ROUND(ssi_unbilled_patient.ssi_financial_class_code, 0, 'ROUND_HALF_EVEN') AS ssi_financial_class_code,
    ssi_unbilled_patient.ssi_acct_type_desc,
    ssi_unbilled_patient.ssi_iplan_id,
    ssi_unbilled_patient.ssi_current_payor_ind,
    ssi_unbilled_patient.request_created_date,
    ssi_unbilled_patient.queue_assigned_date,
    ssi_unbilled_patient.last_activity_date,
    ssi_unbilled_patient.source_system_code,
    ssi_unbilled_patient.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ssi_unbilled_patient
;
