-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/erequest_productivity_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.erequest_productivity_dtl
   OPTIONS(description='This table helps business users manage and analyze productivity for SSC staff as they work through the revenue cycle operations.')
  AS SELECT
      erequest_productivity_dtl.erequest_id,
      erequest_productivity_dtl.note_date_time,
      ROUND(erequest_productivity_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      erequest_productivity_dtl.iplan_id,
      ROUND(erequest_productivity_dtl.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      erequest_productivity_dtl.coid,
      ROUND(erequest_productivity_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      erequest_productivity_dtl.patient_type_code_pos1,
      erequest_productivity_dtl.company_code,
      erequest_productivity_dtl.discharge_date,
      erequest_productivity_dtl.admission_date,
      erequest_productivity_dtl.from_queue_dept_id,
      erequest_productivity_dtl.to_queue_dept_id,
      erequest_productivity_dtl.unbilled_reason_code,
      erequest_productivity_dtl.user_action_type_code,
      erequest_productivity_dtl.request_created_date,
      ROUND(erequest_productivity_dtl.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
      ROUND(erequest_productivity_dtl.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      erequest_productivity_dtl.notes_desc,
      erequest_productivity_dtl.user_id,
      erequest_productivity_dtl.user_full_name,
      erequest_productivity_dtl.source_system_code,
      erequest_productivity_dtl.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.erequest_productivity_dtl
  ;
