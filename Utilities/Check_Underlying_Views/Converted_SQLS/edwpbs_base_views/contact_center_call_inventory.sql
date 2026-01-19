-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/contact_center_call_inventory.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.contact_center_call_inventory
   OPTIONS(description='Inventory of contact center call statistics from various applications like PRIMAS, CMS, CBA etc containing call volume and response statistics.')
  AS SELECT
      contact_center_call_inventory.rptg_date,
      contact_center_call_inventory.call_hour,
      contact_center_call_inventory.data_source_code,
      contact_center_call_inventory.application_id,
      contact_center_call_inventory.call_received_cnt,
      contact_center_call_inventory.call_answered_cnt,
      contact_center_call_inventory.call_sent_to_voice_mail_cnt,
      contact_center_call_inventory.call_abandoned_cnt,
      contact_center_call_inventory.call_abandon_delay_time,
      contact_center_call_inventory.call_abandon_delay_max_time,
      contact_center_call_inventory.call_answer_delay_time,
      contact_center_call_inventory.call_answer_delay_max_time,
      contact_center_call_inventory.call_offered_cnt,
      contact_center_call_inventory.call_accepted_cnt,
      contact_center_call_inventory.call_successful_attempt_cnt,
      contact_center_call_inventory.call_unsuccessful_attempt_cnt,
      contact_center_call_inventory.call_talk_time,
      contact_center_call_inventory.source_system_code,
      contact_center_call_inventory.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.contact_center_call_inventory
  ;
