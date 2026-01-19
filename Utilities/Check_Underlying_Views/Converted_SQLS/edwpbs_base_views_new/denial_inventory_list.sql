-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/denial_inventory_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.denial_inventory_list AS SELECT
    denial_inventory_list.patient_dw_id,
    denial_inventory_list.denial_escalation_unique_id,
    denial_inventory_list.iplan_id,
    denial_inventory_list.iplan_insurance_order_num,
    denial_inventory_list.month_id,
    denial_inventory_list.company_code,
    denial_inventory_list.coid,
    denial_inventory_list.unit_num,
    denial_inventory_list.pat_acct_num,
    denial_inventory_list.project_date,
    denial_inventory_list.automated_letter_sent_date_time,
    denial_inventory_list.write_off_amt,
    denial_inventory_list.total_acct_payment_amt,
    denial_inventory_list.acct_closed_ind,
    denial_inventory_list.acct_closed_date,
    denial_inventory_list.acct_closed_reason_txt,
    denial_inventory_list.assigned_attorney_name,
    denial_inventory_list.attornery_letter_date,
    denial_inventory_list.attornery_assigned_date,
    denial_inventory_list.attorney_status_code,
    denial_inventory_list.attorney_status_date,
    denial_inventory_list.claim_ref_num,
    denial_inventory_list.rebilled_ind,
    denial_inventory_list.rebill_date,
    denial_inventory_list.eligible_for_rebill_ind,
    denial_inventory_list.different_iplan_review_ind,
    denial_inventory_list.inventory_type_name,
    denial_inventory_list.source_system_code,
    denial_inventory_list.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.denial_inventory_list
;
