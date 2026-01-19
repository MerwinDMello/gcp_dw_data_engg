-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/parallon_client_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.parallon_client_detail AS SELECT
    parallon_client_detail.coid,
    parallon_client_detail.unit_num,
    parallon_client_detail.go_live_date,
    parallon_client_detail.ssc,
    parallon_client_detail.conversion_type,
    parallon_client_detail.company_code,
    parallon_client_detail.client_facility_id,
    parallon_client_detail.inbound_gl_ind
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.parallon_client_detail
;
