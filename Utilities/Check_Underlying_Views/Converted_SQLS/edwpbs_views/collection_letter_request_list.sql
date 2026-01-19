-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_letter_request_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.collection_letter_request_list
   OPTIONS(description='Stores the lists the Artiva letters sent and to track how effective they are.')
  AS SELECT
      a.artiva_instance_code,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.artiva_letter_num,
      a.artiva_encounter_num,
      a.letter_sent_date_time,
      a.letter_code,
      a.letter_name,
      a.coid,
      a.company_code,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.letter_action_code,
      a.iplan_id,
      a.iplan_insurance_order_num,
      a.unit_num,
      ROUND(a.artiva_liability_num, 0, 'ROUND_HALF_EVEN') AS artiva_liability_num,
      a.free_form_ind,
      a.letter_tracking_id,
      ROUND(a.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      a.user_id,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_letter_request_list AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
