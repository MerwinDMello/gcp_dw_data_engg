-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
