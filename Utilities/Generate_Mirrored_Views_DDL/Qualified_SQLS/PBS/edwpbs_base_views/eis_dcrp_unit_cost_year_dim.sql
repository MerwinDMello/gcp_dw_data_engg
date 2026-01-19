-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_dcrp_unit_cost_year_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_dcrp_unit_cost_year_dim AS SELECT
    eis_dcrp_unit_cost_year_dim.unit_num_sid,
    eis_dcrp_unit_cost_year_dim.year_created_sid,
    eis_dcrp_unit_cost_year_dim.source_sid,
    eis_dcrp_unit_cost_year_dim.company_code,
    eis_dcrp_unit_cost_year_dim.coid,
    eis_dcrp_unit_cost_year_dim.cost_year_beg_date,
    eis_dcrp_unit_cost_year_dim.cost_year_end_date
  FROM
    `hca-hin-curated-mirroring-td`.edwpbs.eis_dcrp_unit_cost_year_dim
;
