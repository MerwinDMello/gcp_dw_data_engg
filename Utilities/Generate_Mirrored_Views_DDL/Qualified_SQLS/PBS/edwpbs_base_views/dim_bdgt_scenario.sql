-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_bdgt_scenario.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW `hca-hin-prod-cur-parallon`.edwpbs_base_views_copy.dim_bdgt_scenario
   OPTIONS(description='Dimension table that captures the hierarchy and number of levels of an Scenario')
  AS SELECT
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
      `hca-hin-curated-mirroring-td`.edwpbs.dim_bdgt_scenario
  ;
