-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/collection_letter_request_list.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.collection_letter_request_list AS SELECT
    a.artiva_instance_code,
    a.patient_dw_id,
    a.artiva_letter_num,
    a.artiva_encounter_num,
    a.letter_sent_date_time,
    a.letter_code,
    a.letter_name,
    a.coid,
    a.company_code,
    a.pat_acct_num,
    a.letter_action_code,
    a.iplan_id,
    a.iplan_insurance_order_num,
    a.unit_num,
    a.artiva_liability_num,
    a.free_form_ind,
    a.letter_tracking_id,
    a.account_balance_amt,
    a.user_id,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.collection_letter_request_list AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
