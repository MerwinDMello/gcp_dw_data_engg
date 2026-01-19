-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_dcrp_unit_cost_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_dcrp_unit_cost_year_dim AS SELECT
    eis_dcrp_unit_cost_year_dim.unit_num_sid,
    eis_dcrp_unit_cost_year_dim.year_created_sid,
    eis_dcrp_unit_cost_year_dim.source_sid,
    eis_dcrp_unit_cost_year_dim.company_code,
    eis_dcrp_unit_cost_year_dim.coid,
    eis_dcrp_unit_cost_year_dim.cost_year_beg_date,
    eis_dcrp_unit_cost_year_dim.cost_year_end_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.eis_dcrp_unit_cost_year_dim
;
