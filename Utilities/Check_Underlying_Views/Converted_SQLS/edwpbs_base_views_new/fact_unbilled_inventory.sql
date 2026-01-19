-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/fact_unbilled_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_unbilled_inventory AS SELECT
    fact_unbilled_inventory.rptg_date,
    fact_unbilled_inventory.patient_dw_id,
    fact_unbilled_inventory.claim_id,
    fact_unbilled_inventory.request_id,
    fact_unbilled_inventory.queue_dept_id,
    fact_unbilled_inventory.unbilled_status_code,
    fact_unbilled_inventory.unbilled_reason_code,
    fact_unbilled_inventory.him_unbilled_reason_code,
    fact_unbilled_inventory.acct_type_desc,
    fact_unbilled_inventory.request_created_date,
    fact_unbilled_inventory.queue_assigned_date,
    fact_unbilled_inventory.last_activity_date,
    fact_unbilled_inventory.pat_acct_num,
    fact_unbilled_inventory.coid,
    fact_unbilled_inventory.company_code,
    fact_unbilled_inventory.unit_num,
    fact_unbilled_inventory.final_bill_date,
    fact_unbilled_inventory.discharge_date,
    fact_unbilled_inventory.admission_date,
    fact_unbilled_inventory.patient_type_code_pos1,
    fact_unbilled_inventory.payor_sid,
    fact_unbilled_inventory.payor_dw_id_ins1,
    fact_unbilled_inventory.iplan_id_ins1,
    fact_unbilled_inventory.payor_financial_class_sid,
    fact_unbilled_inventory.financial_class_code,
    fact_unbilled_inventory.patient_person_dw_id,
    fact_unbilled_inventory.gross_charge_amt,
    fact_unbilled_inventory.alert_gross_charge_amt,
    fact_unbilled_inventory.total_account_balance_amt,
    fact_unbilled_inventory.alert_total_account_balance_amt,
    fact_unbilled_inventory.rh_total_charge_amt,
    fact_unbilled_inventory.hold_bill_reason_code,
    fact_unbilled_inventory.account_process_ind,
    fact_unbilled_inventory.unbilled_responsibility_ind,
    fact_unbilled_inventory.claim_submit_date,
    fact_unbilled_inventory.final_bill_hold_ind,
    fact_unbilled_inventory.bill_type_code,
    fact_unbilled_inventory.date_of_claim,
    fact_unbilled_inventory.request_file_id,
    fact_unbilled_inventory.source_system_code,
    fact_unbilled_inventory.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.fact_unbilled_inventory
;
