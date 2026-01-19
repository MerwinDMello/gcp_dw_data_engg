-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_unbilled_inventory AS SELECT
    a.rptg_date,
    a.patient_dw_id,
    a.claim_id,
    a.request_id,
    a.queue_dept_id,
    a.unbilled_status_code,
    a.unbilled_reason_code,
    a.him_unbilled_reason_code,
    a.acct_type_desc,
    a.request_created_date,
    a.queue_assigned_date,
    a.last_activity_date,
    a.pat_acct_num,
    a.coid,
    a.company_code,
    a.unit_num,
    a.final_bill_date,
    a.discharge_date,
    a.admission_date,
    a.patient_type_code_pos1,
    a.payor_sid,
    a.payor_dw_id_ins1,
    a.iplan_id_ins1,
    a.payor_financial_class_sid,
    a.financial_class_code,
    a.patient_person_dw_id,
    a.gross_charge_amt,
    a.alert_gross_charge_amt,
    a.total_account_balance_amt,
    a.alert_total_account_balance_amt,
    a.rh_total_charge_amt,
    a.hold_bill_reason_code,
    a.account_process_ind,
    a.unbilled_responsibility_ind,
    a.claim_submit_date,
    a.final_bill_hold_ind,
    a.bill_type_code,
    a.date_of_claim,
    a.request_file_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_unbilled_inventory AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
