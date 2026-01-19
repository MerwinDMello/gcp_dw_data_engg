-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_request_file.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_request_file
   OPTIONS(description='Reference table to describe Unbilled Request File Type')
  AS SELECT
      ref_request_file.request_file_id,
      ref_request_file.file_name,
      ref_request_file.file_location_name,
      ref_request_file.source_system_code,
      ref_request_file.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_request_file
  ;
