-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/erequest_productivity_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.erequest_productivity_dtl AS SELECT
    erequest_productivity_dtl.erequest_id,
    erequest_productivity_dtl.note_date_time,
    erequest_productivity_dtl.patient_dw_id,
    erequest_productivity_dtl.iplan_id,
    erequest_productivity_dtl.financial_class_code,
    erequest_productivity_dtl.coid,
    erequest_productivity_dtl.pat_acct_num,
    erequest_productivity_dtl.patient_type_code_pos1,
    erequest_productivity_dtl.company_code,
    erequest_productivity_dtl.discharge_date,
    erequest_productivity_dtl.admission_date,
    erequest_productivity_dtl.from_queue_dept_id,
    erequest_productivity_dtl.to_queue_dept_id,
    erequest_productivity_dtl.unbilled_reason_code,
    erequest_productivity_dtl.user_action_type_code,
    erequest_productivity_dtl.request_created_date,
    erequest_productivity_dtl.claim_charge_amt,
    erequest_productivity_dtl.total_charge_amt,
    erequest_productivity_dtl.notes_desc,
    erequest_productivity_dtl.user_id,
    erequest_productivity_dtl.user_full_name,
    erequest_productivity_dtl.source_system_code,
    erequest_productivity_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.erequest_productivity_dtl
;
