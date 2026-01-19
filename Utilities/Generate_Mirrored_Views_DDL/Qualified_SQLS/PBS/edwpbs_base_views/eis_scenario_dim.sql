-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_scenario_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.eis_scenario_dim
   OPTIONS(description='Dimension table that captures the Formula and descriptions of various Scenario members for Essbase Cubes')
  AS SELECT
      eis_scenario_dim.scenario_sid,
      eis_scenario_dim.scenario_member,
      eis_scenario_dim.scenario_alias,
      eis_scenario_dim.scenario_info,
      eis_scenario_dim.scenario_formula,
      eis_scenario_dim.dw_last_update_date_time
    FROM
      `hca-hin-curated-mirroring-td`.edwpbs.eis_scenario_dim
  ;
