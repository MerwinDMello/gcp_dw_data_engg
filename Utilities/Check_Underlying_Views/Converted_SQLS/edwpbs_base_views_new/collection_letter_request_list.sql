-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/collection_letter_request_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_letter_request_list AS SELECT
    collection_letter_request_list.artiva_instance_code,
    collection_letter_request_list.patient_dw_id,
    collection_letter_request_list.artiva_letter_num,
    collection_letter_request_list.artiva_encounter_num,
    collection_letter_request_list.letter_sent_date_time,
    collection_letter_request_list.letter_code,
    collection_letter_request_list.letter_name,
    collection_letter_request_list.coid,
    collection_letter_request_list.company_code,
    collection_letter_request_list.pat_acct_num,
    collection_letter_request_list.letter_action_code,
    collection_letter_request_list.iplan_id,
    collection_letter_request_list.iplan_insurance_order_num,
    collection_letter_request_list.unit_num,
    collection_letter_request_list.artiva_liability_num,
    collection_letter_request_list.free_form_ind,
    collection_letter_request_list.letter_tracking_id,
    collection_letter_request_list.account_balance_amt,
    collection_letter_request_list.user_id,
    collection_letter_request_list.source_system_code,
    collection_letter_request_list.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.collection_letter_request_list
;
