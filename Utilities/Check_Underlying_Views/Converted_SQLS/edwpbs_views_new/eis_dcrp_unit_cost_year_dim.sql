-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/eis_dcrp_unit_cost_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.eis_dcrp_unit_cost_year_dim AS SELECT
    a.coid,
    a.company_code,
    a.cost_year_beg_date,
    a.source_sid,
    a.unit_num_sid,
    a.year_created_sid,
    a.cost_year_end_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS a
    CROSS JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b
  WHERE b.co_id = a.coid
   AND b.company_code = a.company_code
   AND b.user_id = session_user()
;
