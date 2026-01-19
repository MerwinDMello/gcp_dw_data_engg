-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_remittance_oth_subscriber_info.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.ref_remittance_oth_subscriber_info
   OPTIONS(description='Reference table to maintain the Other Subscriber details of the claims sent.')
  AS SELECT
      ref_remittance_oth_subscriber_info.remittance_oth_subscriber_sid,
      ref_remittance_oth_subscriber_info.oth_subscriber_id_qualifier_code,
      ref_remittance_oth_subscriber_info.oth_subscriber_id,
      ref_remittance_oth_subscriber_info.oth_subscriber_enty_type_qualifier_code,
      ref_remittance_oth_subscriber_info.oth_subscriber_last_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_first_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_middle_name,
      ref_remittance_oth_subscriber_info.oth_subscriber_name_suffix,
      ref_remittance_oth_subscriber_info.source_system_code,
      ref_remittance_oth_subscriber_info.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.ref_remittance_oth_subscriber_info
  ;
