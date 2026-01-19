-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_collection_agency_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_collection_agency_dim AS SELECT
    a.account_status_sid - 7000 AS collection_agency_sid,
    a.account_status_name_child AS collection_agency_member,
    a.account_status_name_parent AS collection_agency_gen02,
    a.account_status_child_alias_name AS collection_agency_gen02_alias,
    CASE
      WHEN a.sort_key_num = 9999 THEN a.sort_key_num
      ELSE a.sort_key_num - 2
    END AS collection_agency_gen02_sort
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_account_status AS a
  WHERE a.account_status_sid > 7000
;
