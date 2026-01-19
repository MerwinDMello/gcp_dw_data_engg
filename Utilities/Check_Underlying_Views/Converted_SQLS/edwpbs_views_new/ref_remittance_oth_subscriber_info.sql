-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_remittance_oth_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_remittance_oth_subscriber_info AS SELECT
    a.remittance_oth_subscriber_sid,
    a.oth_subscriber_id_qualifier_code,
    a.oth_subscriber_id,
    a.oth_subscriber_enty_type_qualifier_code,
    a.oth_subscriber_last_name,
    a.oth_subscriber_first_name,
    a.oth_subscriber_middle_name,
    a.oth_subscriber_name_suffix,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_remittance_oth_subscriber_info AS a
;
