-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/parallon_client_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.parallon_client_detail AS SELECT
    parallon_client_detail.coid,
    parallon_client_detail.unit_num,
    parallon_client_detail.go_live_date,
    parallon_client_detail.ssc,
    parallon_client_detail.conversion_type,
    parallon_client_detail.company_code,
    parallon_client_detail.client_facility_id,
    parallon_client_detail.inbound_gl_ind
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.parallon_client_detail
;
