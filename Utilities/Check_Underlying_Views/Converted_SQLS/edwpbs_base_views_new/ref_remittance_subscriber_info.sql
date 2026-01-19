-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_subscriber_info AS SELECT
    ref_remittance_subscriber_info.remittance_subscriber_sid,
    ref_remittance_subscriber_info.patient_health_ins_num,
    ref_remittance_subscriber_info.insured_identification_qualifier_code,
    ref_remittance_subscriber_info.subscriber_id,
    ref_remittance_subscriber_info.insured_entity_type_qualifier_code,
    ref_remittance_subscriber_info.subscriber_last_name,
    ref_remittance_subscriber_info.subscriber_first_name,
    ref_remittance_subscriber_info.subscriber_middle_name,
    ref_remittance_subscriber_info.subscriber_name_suffix,
    ref_remittance_subscriber_info.source_system_code,
    ref_remittance_subscriber_info.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_remittance_subscriber_info
;
