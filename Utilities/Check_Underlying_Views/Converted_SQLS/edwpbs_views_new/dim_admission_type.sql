-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_admission_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_admission_type AS SELECT
    a.admission_type_sid,
    a.aso_bso_storage_code,
    a.admission_type_parent,
    a.admission_type_child,
    a.admission_type_alias_name,
    a.sort_key_num,
    a.two_pass_calc_code,
    a.consolidation_code,
    a.storage_code,
    a.formula_text,
    a.member_solve_order_num,
    a.admission_type_hier_name,
    a.admission_type_name_uda
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_admission_type AS a
;
