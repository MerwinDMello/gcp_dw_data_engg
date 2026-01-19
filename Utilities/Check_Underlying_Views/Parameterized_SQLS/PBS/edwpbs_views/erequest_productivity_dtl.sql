-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/erequest_productivity_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.erequest_productivity_dtl
   OPTIONS(description='This table helps business users manage and analyze productivity for SSC staff as they work through the revenue cycle operations.')
  AS SELECT
      a.erequest_id,
      a.note_date_time,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.iplan_id,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.coid,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.patient_type_code_pos1,
      a.company_code,
      a.discharge_date,
      a.admission_date,
      a.from_queue_dept_id,
      a.to_queue_dept_id,
      a.unbilled_reason_code,
      a.user_action_type_code,
      a.request_created_date,
      ROUND(a.claim_charge_amt, 3, 'ROUND_HALF_EVEN') AS claim_charge_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      a.notes_desc,
      a.user_id,
      a.user_full_name,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.erequest_productivity_dtl AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
