-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_center_call_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_center_call_inventory AS SELECT
    a.rptg_date,
    a.call_hour,
    a.data_source_code,
    a.application_id,
    a.call_received_cnt,
    a.call_answered_cnt,
    a.call_sent_to_voice_mail_cnt,
    a.call_abandoned_cnt,
    a.call_abandon_delay_time,
    a.call_abandon_delay_max_time,
    a.call_answer_delay_time,
    a.call_answer_delay_max_time,
    a.call_offered_cnt,
    a.call_accepted_cnt,
    a.call_successful_attempt_cnt,
    a.call_unsuccessful_attempt_cnt,
    a.call_talk_time,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_center_call_inventory AS a
;
