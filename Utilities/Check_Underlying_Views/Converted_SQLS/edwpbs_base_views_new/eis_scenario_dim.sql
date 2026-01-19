-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/eis_scenario_dim.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.eis_scenario_dim AS SELECT
    eis_scenario_dim.scenario_sid,
    eis_scenario_dim.scenario_member,
    eis_scenario_dim.scenario_alias,
    eis_scenario_dim.scenario_info,
    eis_scenario_dim.scenario_formula,
    eis_scenario_dim.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.eis_scenario_dim
;
