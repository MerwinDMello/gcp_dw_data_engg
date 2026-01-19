-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_scenario.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_bdgt_scenario AS SELECT
    dim_bdgt_scenario.scenario_sid,
    dim_bdgt_scenario.scenario_hier_name,
    dim_bdgt_scenario.scenario_name_parent,
    dim_bdgt_scenario.scenario_name_child,
    dim_bdgt_scenario.scenario_alias_name,
    dim_bdgt_scenario.aso_bso_storage_code,
    dim_bdgt_scenario.sort_key_num,
    dim_bdgt_scenario.consolidation_code,
    dim_bdgt_scenario.storage_code,
    dim_bdgt_scenario.two_pass_calc_code,
    dim_bdgt_scenario.formula_text,
    dim_bdgt_scenario.member_solve_order_num,
    dim_bdgt_scenario.scenario_uda_name,
    dim_bdgt_scenario.source_system_code,
    dim_bdgt_scenario.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_bdgt_scenario
;
