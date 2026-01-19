-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/denial_inventory_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.denial_inventory_list AS SELECT
    a.patient_dw_id,
    a.denial_escalation_unique_id,
    a.iplan_id,
    a.iplan_insurance_order_num,
    a.month_id,
    a.company_code,
    a.coid,
    a.unit_num,
    a.pat_acct_num,
    a.project_date,
    a.automated_letter_sent_date_time,
    a.write_off_amt,
    a.total_acct_payment_amt,
    a.acct_closed_ind,
    a.acct_closed_date,
    a.acct_closed_reason_txt,
    a.assigned_attorney_name,
    a.attornery_letter_date,
    a.attornery_assigned_date,
    a.attorney_status_code,
    a.attorney_status_date,
    a.claim_ref_num,
    a.rebilled_ind,
    a.rebill_date,
    a.eligible_for_rebill_ind,
    a.different_iplan_review_ind,
    a.inventory_type_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.denial_inventory_list AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
