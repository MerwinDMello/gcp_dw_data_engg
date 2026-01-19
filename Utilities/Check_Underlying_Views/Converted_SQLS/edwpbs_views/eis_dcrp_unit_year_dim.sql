-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_dcrp_unit_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_dcrp_unit_year_dim AS SELECT
    a.unit_num_sid,
    a.year_created_sid,
    a.source_sid,
    a.company_code,
    a.coid,
    a.year_beg_date,
    a.year_end_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_year_dim AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(b.co_id) = upper(a.coid)
     AND upper(b.company_code) = upper(a.company_code)
     AND b.user_id = session_user()
;
