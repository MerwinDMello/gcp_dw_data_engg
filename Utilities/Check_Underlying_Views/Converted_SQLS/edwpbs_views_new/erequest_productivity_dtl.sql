-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/erequest_productivity_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.erequest_productivity_dtl AS SELECT
    a.erequest_id,
    a.note_date_time,
    a.patient_dw_id,
    a.iplan_id,
    a.financial_class_code,
    a.coid,
    a.pat_acct_num,
    a.patient_type_code_pos1,
    a.company_code,
    a.discharge_date,
    a.admission_date,
    a.from_queue_dept_id,
    a.to_queue_dept_id,
    a.unbilled_reason_code,
    a.user_action_type_code,
    a.request_created_date,
    a.claim_charge_amt,
    a.total_charge_amt,
    a.notes_desc,
    a.user_id,
    a.user_full_name,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.erequest_productivity_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
