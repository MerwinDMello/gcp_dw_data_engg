-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_letter_request_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.collection_letter_request_list
   OPTIONS(description='Stores the lists the Artiva letters sent and to track how effective they are.')
  AS SELECT
      collection_letter_request_list.artiva_instance_code,
      ROUND(collection_letter_request_list.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      collection_letter_request_list.artiva_letter_num,
      collection_letter_request_list.artiva_encounter_num,
      collection_letter_request_list.letter_sent_date_time,
      collection_letter_request_list.letter_code,
      collection_letter_request_list.letter_name,
      collection_letter_request_list.coid,
      collection_letter_request_list.company_code,
      ROUND(collection_letter_request_list.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      collection_letter_request_list.letter_action_code,
      collection_letter_request_list.iplan_id,
      collection_letter_request_list.iplan_insurance_order_num,
      collection_letter_request_list.unit_num,
      ROUND(collection_letter_request_list.artiva_liability_num, 0, 'ROUND_HALF_EVEN') AS artiva_liability_num,
      collection_letter_request_list.free_form_ind,
      collection_letter_request_list.letter_tracking_id,
      ROUND(collection_letter_request_list.account_balance_amt, 3, 'ROUND_HALF_EVEN') AS account_balance_amt,
      collection_letter_request_list.user_id,
      collection_letter_request_list.source_system_code,
      collection_letter_request_list.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.collection_letter_request_list
  ;
