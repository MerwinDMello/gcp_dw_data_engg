-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_appeal_root_cause.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_appeal_root_cause AS SELECT
    dim_appeal_root_cause.appeal_root_cause_sid,
    dim_appeal_root_cause.aso_bso_storage_code,
    dim_appeal_root_cause.appeal_root_cause_parent,
    dim_appeal_root_cause.appeal_root_cause_child,
    dim_appeal_root_cause.appeal_root_cause_alias_name,
    dim_appeal_root_cause.sort_key_num,
    dim_appeal_root_cause.two_pass_calc_code,
    dim_appeal_root_cause.consolidation_code,
    dim_appeal_root_cause.storage_code,
    dim_appeal_root_cause.formula_text,
    dim_appeal_root_cause.member_solve_order_num,
    dim_appeal_root_cause.appeal_root_cause_hier_name,
    dim_appeal_root_cause.appeal_root_cause_uda_name
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_appeal_root_cause
;
