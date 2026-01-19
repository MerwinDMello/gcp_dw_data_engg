-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/contact_center_call_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.contact_center_call_inventory
   OPTIONS(description='Inventory of contact center call statistics from various applications like PRIMAS, CMS, CBA etc containing call volume and response statistics.')
  AS SELECT
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
